package shed

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

type SortOrder int

const (
	SortOrderAsc  SortOrder = 1
	SortOrderDesc           = -1
)

type SortDef struct {
	Name  string
	Order SortOrder
}

type ColType int

const (
	FloatType ColType = iota + 1
	StringType
)

type Column struct {
	T    ColType
	Name string
}

type TableDef struct {
	Columns []Column
	Order   []SortDef
}

type Table struct {
	d *Driver
	*shedpb.TableDef
}

func (t *Table) NewPartData(rows map[string]*shedpb.DatabaseValues) *PartData {
	return &PartData{
		Def:  t.TableDef,
		Rows: rows,
	}
}

func (t *Table) CreatePart(ctx context.Context, p *PartData) (*Part, error) {
	part := &Part{
		id:    time.Now().UTC().Format(time.RFC3339) + uuid.New().String(),
		table: t,
	}
	err := part.WritePart(ctx, p)
	if err != nil {
		return nil, err
	}
	return part, nil
}

func (t *Table) OpenPart(ctx context.Context, id string) *Part {
	return &Part{
		id:    id,
		table: t,
	}
}

type Encoder interface {
	Encode(v any) error
	Flush() (int64, error)
}

type EncoderFactory func(context.Context, io.Writer) Encoder
type DecoderFactory func(context.Context, io.Reader, func() any) iter.Seq2[any, error]

type Decoder interface {
	Read(context.Context, io.Reader, func() any) iter.Seq2[any, error]
}

type ProtobufEncoder struct {
	w       *ioWriterCounter
	msgs    []proto.Message
	col     *shedpb.DatabaseValues
	marshal func(proto.Message) ([]byte, error)
}

func NewEncoderFactory(ctx context.Context, w io.Writer) Encoder {
	return &ProtobufEncoder{
		w:       &ioWriterCounter{Writer: w},
		marshal: proto.Marshal,
	}
}

var _ Encoder = &ProtobufEncoder{}

// Flush implements Encoder.
func (p *ProtobufEncoder) Flush() (int64, error) {
	if p.col == nil && len(p.msgs) == 0 {
		return 0, nil
	}
	begin := p.w.bytesWritten
	p.msgs = append(p.msgs, p.col)
	varintBuf := make([]byte, 64, 64)
	for _, msg := range p.msgs {
		bs, err := p.marshal(msg)
		if err != nil {
			return 0, err
		}
		size := binary.PutUvarint(varintBuf, uint64(len(bs)))
		_, err = p.w.Write(varintBuf[0:size])
		if err != nil {
			return 0, err
		}

		_, err = p.w.Write(bs)
		if err != nil {
			return 0, err
		}
	}
	p.col = nil
	p.msgs = nil
	return p.w.bytesWritten - begin, nil
}

// Encode implements Encoder.
func (p *ProtobufEncoder) Encode(v any) error {
	switch vv := v.(type) {
	case float64:
		if p.col == nil {
			p.col = &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: []float64{vv}}}}
		} else {
			p.col.GetFloatValues().Values = append(p.col.GetFloatValues().Values, vv)
		}
	case string:
		if p.col == nil {
			p.col = &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: []string{vv}}}}
		} else {
			p.col.GetStringValues().Values = append(p.col.GetStringValues().Values, vv)
		}
	case proto.Message:
		p.msgs = append(p.msgs, vv)

	}
	return nil
}

// var _ Encoder = &ProtobufEncoder{}

var NoYieldError = errors.New("iterator terminated with no output")

func readOne2[T any](seq iter.Seq2[T, error]) (T, error) {
	next, stop := iter.Pull2(seq)
	t, v, ok := next()
	if !ok {
		return t, NoYieldError
	}
	stop()
	return t, v
}

type Driver struct {
	granuleSize    int
	encoderFactory EncoderFactory
	decoderFactory DecoderFactory
	bucket         *blob.Bucket
	prefix         string
}

type IndexEntry struct {
	SortKey []interface{}
	Offset  int
}

func (d *Driver) WritePartColumn(ctx context.Context, w io.Writer, elements iter.Seq[interface{}]) (offsets []int64, err error) {
	defer func() {
		if wc, ok := w.(io.WriteCloser); ok {
			if err2 := wc.Close(); err2 != nil {
				err = errors.Join(err, err2)
			}
		}
	}()
	encoder := d.encoderFactory(ctx, w)
	var indexEntries []int64
	var bytesWritten int64
	var i int64
	for element := range elements {
		if i%int64(d.granuleSize) == 0 {
			bytesInFlush, err := encoder.Flush()
			if err != nil {
				return nil, fmt.Errorf("writing part: %w", err)
			}
			bytesWritten += bytesInFlush
			indexEntries = append(indexEntries, bytesWritten)
		}
		err := encoder.Encode(element)
		if err != nil {
			return nil, fmt.Errorf("writing part: %w", err)
		}

		i++
	}
	_, err = encoder.Flush()
	if err != nil {
		return nil, fmt.Errorf("writing part: %w", err)
	}
	return indexEntries, nil
}

func cmp(a interface{}, b interface{}) int {
	switch aa := a.(type) {
	case float64:
		bb := b.(float64)
		if aa == bb {
			return 0
		}
		if aa < bb {
			return -1
		}
		return 1
	case string:
		bb := b.(string)
		if aa == bb {
			return 0
		}
		if aa < bb {
			return -1
		}
		return 1
	}
	panic("a is not a valid type")
}

type Part struct {
	id          string
	table       *Table
	cachedIndex *shedpb.PartIndex

	sync.Mutex
}

type PartData shedpb.PartData

func (p *PartData) Len() int {
	for _, v := range p.Rows {
		switch vv := v.Value.(type) {
		case *shedpb.DatabaseValues_FloatValues:
			return len(vv.FloatValues.Values)
		case *shedpb.DatabaseValues_StringValues:
			return len(vv.StringValues.Values)
		}
	}
	return 0
}

func indexIntoDbValues(vs *shedpb.DatabaseValues, i int) any {
	switch vv := vs.GetValue().(type) {
	case *shedpb.DatabaseValues_FloatValues:
		return vv.FloatValues.GetValues()[i]
	case *shedpb.DatabaseValues_StringValues:
		return vv.StringValues.GetValues()[i]
	}
	return nil
}

func indexIntoIndex(pi *shedpb.PartIndex, i int) []any {
	var ret []any

	for _, k := range pi.Keys {
		ret = append(ret, indexIntoDbValues(k, i))
	}
	return ret
}

// Less implements sort.Interface.
func (p *PartData) Less(i, j int) bool {
	for _, sortDef := range p.Def.Order {
		if cmped := cmp(indexIntoDbValues(p.Rows[sortDef.Name], i), indexIntoDbValues(p.Rows[sortDef.Name], j)); cmped != 0 {
			return int(sortDef.Order)*cmped < 0
		}
	}
	return false
}

func (t *Table) IndexCmp(a, b []interface{}) int {
	for i, sortDef := range t.Order {
		if a == nil && b == nil {
			return 0
		}
		if a == nil {
			return -1
		}
		if b == nil {
			return 1
		}
		if cmped := cmp(a[i], b[i]); cmped != 0 {
			return int(sortDef.Order) * cmped
		}
	}
	return 0
}

// Swap implements sort.Interface.
func (p *PartData) Swap(i int, j int) {
	for _, elemsContainer := range p.Rows {
		switch elemsContainerV := elemsContainer.Value.(type) {
		case *shedpb.DatabaseValues_FloatValues:
			elems := elemsContainerV.FloatValues.GetValues()
			elems[i], elems[j] = elems[j], elems[i]
		case *shedpb.DatabaseValues_StringValues:
			elems := elemsContainerV.StringValues.GetValues()
			elems[i], elems[j] = elems[j], elems[i]
		}
	}
}

type IOStatistics struct {
	rowsRead     int64
	rowsWritten  int64
	bytesRead    int64
	bytesWritten int64
}

var stats IOStatistics

// Read implements Encoder.
func ProtoDecoderFactory(ctx context.Context, r io.Reader, factory func() any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		bufReader := bufio.NewReader(r)
		for {

			bsToRead, err := binary.ReadUvarint(bufReader)

			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(nil, err)
				return
			}
			buf := make([]byte, bsToRead)
			_, err = io.ReadFull(bufReader, buf)
			if err != nil {
				yield(nil, err)
				return
			}
			f := factory()
			switch ff := f.(type) {
			case proto.Message:
				proto.Unmarshal(buf, ff)
				atomic.AddInt64(&stats.rowsRead, 1)
				if !yield(ff, nil) {
					return
				}
			default:
				vals := &shedpb.DatabaseValues{}
				proto.Unmarshal(buf, vals)
				switch col := vals.Value.(type) {
				case *shedpb.DatabaseValues_FloatValues:
					for _, ret := range col.FloatValues.GetValues() {
						atomic.AddInt64(&stats.rowsRead, 1)
						if !yield(ret, nil) {
							return
						}
					}
				case *shedpb.DatabaseValues_StringValues:
					for _, ret := range col.StringValues.GetValues() {
						atomic.AddInt64(&stats.rowsRead, 1)
						if !yield(ret, nil) {
							return
						}
					}
				}
			}

		}

	}
}

type ioWriterCounter struct {
	io.Writer
	bytesWritten int64
}

func (i *ioWriterCounter) Write(p []byte) (n int, err error) {
	n, err = i.Writer.Write(p)
	i.bytesWritten += int64(n)
	return
}

func (part *Part) GetPartColumnPath(k string) string {
	partName := part.table.d.prefix + part.id
	return path.Join(partName, k+".jsonl")
}

func (part *Part) GetPartIndexPath() string {
	d := part.table.d
	partName := d.prefix + part.id
	return path.Join(partName, "idx.json")
}

func ZipIters(iters []iter.Seq2[any, error]) iter.Seq2[[]any, error] {
	return func(yield func([]any, error) bool) {
		pulls := make([]func() (any, error, bool), len(iters))
		stops := make([]func(), len(iters))
		defer func() {
			for _, stop := range stops {
				if stop != nil {
					stop()
				}
			}
		}()

		for i, iterFunc := range iters {
			pulls[i], stops[i] = iter.Pull2(iterFunc)
		}

		for {
			vals := make([]any, len(iters))
			var err error
			var ok bool
			for i, pull := range pulls {
				vals[i], err, ok = pull()
				if !ok {
					return
				}
				if err != nil {
					yield(nil, err)
					return
				}
			}
			if !yield(vals, nil) {
				return
			}
		}
	}
}

func lenDbVals(vals *shedpb.DatabaseValues) int {
	return max(len(vals.GetFloatValues().GetValues()), len(vals.GetStringValues().GetValues()))
}

func AllDbVals(vals *shedpb.DatabaseValues) iter.Seq[any] {
	return func(yield func(interface{}) bool) {
		switch v := vals.GetValue().(type) {
		case *shedpb.DatabaseValues_FloatValues:
			for _, x := range v.FloatValues.GetValues() {
				if !yield(x) {
					return
				}
			}
		case *shedpb.DatabaseValues_StringValues:
			for _, x := range v.StringValues.GetValues() {
				if !yield(x) {
					return
				}
			}
		}
	}
}

func colMap(def *shedpb.TableDef) map[string]shedpb.ColType {
	ret := make(map[string]shedpb.ColType)
	for _, col := range def.GetColumns() {
		ret[col.GetName()] = col.GetColType()
	}
	return ret
}

func initPartIndex(def *shedpb.TableDef) *shedpb.PartIndex {
	cm := colMap(def)
	var keys []*shedpb.DatabaseValues
	for _, v := range def.GetOrder() {
		ct := cm[v.GetName()]
		switch ct {
		case shedpb.ColType_COL_TYPE_FLOAT:
			keys = append(keys, &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{}}})
		case shedpb.ColType_COL_TYPE_STRING:
			keys = append(keys, &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{}}})
		default:
			panic(ct)
		}
	}
	return &shedpb.PartIndex{
		Keys:    keys,
		Offsets: make(map[string]*shedpb.Offsets),
	}
}

func (part *Part) WritePart(ctx context.Context, partData *PartData) (outErr error) {
	d := part.table.d
	sort.Sort(partData)
	var indicesT []*shedpb.DatabaseValues

	def := part.table
	for _, sortDef := range def.Order {
		indicesT = append(indicesT, partData.Rows[sortDef.Name])
	}
	index := initPartIndex(part.table.TableDef)
	partLen := lenDbVals(indicesT[0])

	for i := 0; i < partLen; i += part.table.d.granuleSize {
		for j, col := range indicesT {
			switch vals := col.GetValue().(type) {
			case *shedpb.DatabaseValues_StringValues:
				index.Keys[j].GetStringValues().Values = append(index.Keys[j].GetStringValues().Values, vals.StringValues.GetValues()[i])
			case *shedpb.DatabaseValues_FloatValues:
				index.Keys[j].GetFloatValues().Values = append(index.Keys[j].GetFloatValues().Values, vals.FloatValues.GetValues()[i])
			}
		}
	}

	offsets := make(map[string]*shedpb.Offsets)
	for k, v := range partData.Rows {
		file, err := d.bucket.NewWriter(ctx, part.GetPartColumnPath(k), nil)
		if err != nil {
			return fmt.Errorf("writing part %s: %w", k, err)
		}
		indexEntries, err := d.WritePartColumn(ctx, file, AllDbVals(v))
		if err != nil {
			return fmt.Errorf("writing part %s: %w", k, err)
		}
		offsets[k] = &shedpb.Offsets{Offsets: indexEntries}

	}

	index.Offsets = offsets

	indexFile, err := d.bucket.NewWriter(ctx, part.GetPartIndexPath(), nil)
	defer func() {
		if err := indexFile.Close(); err != nil {
			outErr = errors.Join(err)
		}
	}()
	if err != nil {
		return fmt.Errorf("writing part: %w", err)
	}
	encoder := d.encoderFactory(ctx, indexFile)
	if err := encoder.Encode(index); err != nil {
		return fmt.Errorf("writing part: %w", err)
	}
	if _, err := encoder.Flush(); err != nil {
		return fmt.Errorf("writing part: %w", err)
	}
	return nil
}

func appendKeyRow(pi *shedpb.PartIndex, vals []any) {
	for i, keyCol := range pi.Keys {
		switch col := keyCol.Value.(type) {
		case *shedpb.DatabaseValues_FloatValues:
			col.FloatValues.Values = append(col.FloatValues.Values, vals[i].(float64))
		case *shedpb.DatabaseValues_StringValues:
			col.StringValues.Values = append(col.StringValues.Values, vals[i].(string))
		}
	}
}

func (t *Table) MergeParts(ctx context.Context, a, b *Part) (p *Part, outErr error) {

	p = &Part{
		id:    time.Now().UTC().Format(time.RFC3339) + uuid.New().String(),
		table: t,
	}
	var colChannels []chan interface{}
	eg, ctxGroup := errgroup.WithContext(ctx)
	pi := initPartIndex(t.TableDef)
	var piLock sync.Mutex
	for _, col := range t.Columns {
		col := col
		c := make(chan interface{})
		colChannels = append(colChannels, c)
		eg.Go(
			func() error {
				file, err := t.d.bucket.NewWriter(ctxGroup, p.GetPartColumnPath(col.Name), nil)
				defer file.Close()
				if err != nil {
					return fmt.Errorf("writing part %s: %w", col.Name, err)
				}
				indexEntries, err := t.d.WritePartColumn(ctxGroup, file, func(yield func(interface{}) bool) {
					for val := range c {
						if !yield(val) {
							return
						}
					}
				})
				if err != nil {
					return err
				}
				piLock.Lock()
				defer piLock.Unlock()
				pi.Offsets[col.Name] = &shedpb.Offsets{Offsets: indexEntries}
				return nil
			},
		)
	}

	idxIterA, colItersA := a.IndexColumnsIterators(ctxGroup)
	idxIterB, colItersB := b.IndexColumnsIterators(ctxGroup)

	pullIdxIterA, stopIdxIterA := iter.Pull2(idxIterA)
	defer stopIdxIterA()
	pullIdxIterB, stopIdxIterB := iter.Pull2(idxIterB)
	defer stopIdxIterB()

	pullColIterA, stopColIterA := iter.Pull2(colItersA)
	defer stopColIterA()
	pullColIterB, stopColIterB := iter.Pull2(colItersB)
	defer stopColIterB()

	valA, errA, okA := pullIdxIterA()
	valB, errB, okB := pullIdxIterB()

	ctr := -1
	for {
		ctr += 1
		if errA != nil {
			return nil, fmt.Errorf("merging parts: %w", errA)
		}
		if errB != nil {
			return nil, fmt.Errorf("merging parts: %w", errB)
		}
		if !okA && !okB {
			break
		}
		if !okA || okB && t.IndexCmp(valA, valB) > 0 {
			val, err, ok := pullColIterB()
			if !ok {
				return nil, fmt.Errorf("merging parts: %w", NoYieldError)
			}
			if err != nil {
				return nil, fmt.Errorf("merging parts: %w", err)
			}
			for i, v := range val {
				colChannels[i] <- v
			}
			if ctr%t.d.granuleSize == 0 {
				fmt.Println("")
				appendKeyRow(pi, valB)
			}
			valB, errB, okB = pullIdxIterB()
			continue
		}
		val, err, ok := pullColIterA()
		if !ok {
			return nil, fmt.Errorf("merging parts: %w", NoYieldError)
		}
		if err != nil {
			return nil, fmt.Errorf("merging parts: %w", err)
		}
		for i, v := range val {
			colChannels[i] <- v
		}
		if ctr%t.d.granuleSize == 0 {
			appendKeyRow(pi, valA)
		}
		valA, errA, okA = pullIdxIterA()
	}
	for _, c := range colChannels {
		close(c)
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	indexFile, err := t.d.bucket.NewWriter(ctx, p.GetPartIndexPath(), nil)
	if err != nil {
		return nil, fmt.Errorf("writing part: %w", err)
	}
	defer func() {
		if err := indexFile.Close(); err != nil {
			outErr = errors.Join(err)
		}
	}()
	encoder := t.d.encoderFactory(ctx, indexFile)
	err = encoder.Encode(pi)
	if err != nil {
		return nil, fmt.Errorf("writing part: %w", err)
	}
	if _, err := encoder.Flush(); err != nil {
		return nil, fmt.Errorf("writing part: %w", err)
	}
	return p, nil
}

func (part *Part) ScanColumn(ctx context.Context, column string, offset int64, l int64) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		f, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(column), offset, l, nil)
		defer f.Close()
		if err != nil {
			yield(nil, err)
			return
		}
		for r, e := range part.table.d.decoderFactory(ctx, f, func() interface{} { return nil }) {
			if !yield(r, e) {
				return
			}
		}
	}
}

func (part *Part) IndexColumnsIterators(ctx context.Context) (iter.Seq2[[]any, error], iter.Seq2[[]any, error]) {
	var idxs []iter.Seq2[any, error]
	for _, o := range part.table.Order {
		idxs = append(idxs, part.ScanColumn(ctx, o.Name, 0, -1))
	}
	var rets []iter.Seq2[any, error]

	for _, col := range part.table.Columns {
		rets = append(rets, part.ScanColumn(ctx, col.Name, 0, -1))
	}

	return ZipIters(idxs), ZipIters(rets)
}

func (part *Part) ScanColumnRange(ctx context.Context, column string, minIndex, maxIndex []interface{}) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		index, err := part.LoadIndex(ctx)
		if err != nil {
			yield(nil, err)
			return
		}

		var offsetIdx int
		if minIndex != nil {
			offsetIdx, err = part.IndexLeq(ctx, minIndex)
			if err != nil {
				yield(nil, err)
				return
			}
			if offsetIdx == -1 {
				return
			}
		}

		numIndices := len(part.table.Order)

		var idxReaders []*blob.Reader
		pullers := make([]func() (any, error, bool), numIndices)
		stoppers := make([]func(), numIndices)
		defer func() {
			for _, idxReader := range idxReaders {
				idxReader.Close()
			}
			for _, stopper := range stoppers {
				if stopper != nil {
					stopper()
				}
			}
		}()

		for i, o := range part.table.Order {
			reader, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(o.Name), index.GetOffsets()[o.Name].GetOffsets()[offsetIdx], -1, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			idxReaders = append(idxReaders, reader)
			decoder := part.table.d.decoderFactory(ctx, reader, func() any { return nil })
			pullers[i], stoppers[i] = iter.Pull2(decoder)

		}

		offset := index.GetOffsets()[column].GetOffsets()[offsetIdx]
		f, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(column), offset, -1, nil)

		if err != nil {
			yield(nil, err)
			return
		}
		defer f.Close()

		idxElems := make([]interface{}, len(part.table.Order))
		for r, e := range part.table.d.decoderFactory(ctx, f, func() interface{} { return nil }) {
			for i, f := range pullers {
				idxElem, err, ok := f()
				if !ok {
					yield(nil, NoYieldError)
					return
				}
				if err != nil {
					yield(nil, fmt.Errorf("scanning range: %w", err))
					return
				}
				idxElems[i] = idxElem
			}

			if minIndex != nil && part.table.IndexCmp(minIndex, idxElems) > 0 {
				continue
			}
			if maxIndex != nil && part.table.IndexCmp(maxIndex, idxElems) < 0 {
				return
			}

			if !yield(r, e) {
				return
			}
		}
	}
}

func (part *Part) IndexLeq(ctx context.Context, min []interface{}) (int, error) {
	idx, err := part.LoadIndex(ctx)
	if err != nil {
		return 0, fmt.Errorf("index leq: %w", err)
	}
	i := part.table.binarySearch(idx, min, 0, lenDbVals(idx.Keys[0])-1)
	return i, nil
}

func (t *Table) binarySearch(partIndex *shedpb.PartIndex, min []interface{}, low, high int) int {
	if high <= low {
		if t.IndexCmp(indexIntoIndex(partIndex, low), min) <= 0 {
			return low
		}
		if low == 0 {
			return -1
		}
		return low - 1
	}

	mid := (low + high) / 2
	if c := t.IndexCmp(indexIntoIndex(partIndex, mid), min); c < 0 {
		return t.binarySearch(partIndex, min, mid+1, high)
	} else if c > 0 {
		return t.binarySearch(partIndex, min, low, mid-1)
	} else {
		return t.binarySearch(partIndex, min, low, mid)
	}
}

func (part *Part) LoadIndex(ctx context.Context) (*shedpb.PartIndex, error) {
	part.Lock()
	defer part.Unlock()
	if part.cachedIndex != nil {
		return part.cachedIndex, nil
	}
	f, err := part.table.d.bucket.NewReader(ctx, part.GetPartIndexPath(), nil)
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	ret, err := readOne2(part.table.d.decoderFactory(ctx, f, func() any { return new(shedpb.PartIndex) }))
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	part.cachedIndex = ret.(*shedpb.PartIndex)
	return ret.(*shedpb.PartIndex), nil
}
