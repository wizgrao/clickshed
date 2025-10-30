package shed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
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

type Table struct {
	d       *Driver
	Columns []Column
	Order   []SortDef
}

func (t *Table) NewPartData(rows map[string][]interface{}) *PartData {
	return &PartData{
		def:  t,
		rows: rows,
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
}

// Encode implements Encoder.
func (p *ProtobufEncoder) Encode(v any) ([]byte, error) {
	switch v.(type) {
	case float64:

	}
	return nil, nil
}

// Read implements Encoder.
func (p *ProtobufEncoder) Read(context.Context, io.Reader, func() any) iter.Seq2[any, error] {
	panic("unimplemented")
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
	cachedIndex *PartIndex

	sync.Mutex
}

type PartData struct {
	def  *Table
	rows map[string][]interface{}
}

// Len implements sort.Interface.
func (p *PartData) Len() int {
	for _, v := range p.rows {
		return len(v)
	}
	return 0
}

// Less implements sort.Interface.
func (p *PartData) Less(i, j int) bool {
	for _, sortDef := range p.def.Order {

		if cmped := cmp(p.rows[sortDef.Name][i], p.rows[sortDef.Name][j]); cmped != 0 {
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
	for _, elems := range p.rows {
		elems[i], elems[j] = elems[j], elems[i]
	}
}

type IOStatistics struct {
	rowsRead     int64
	rowsWritten  int64
	bytesRead    int64
	bytesWritten int64
}

var stats IOStatistics

type JsonEncoder struct {
	Prefix string
	Indent string

	err   error
	stats IOStatistics
	w     *ioWriterCounter
	*json.Encoder
}

// Flush implements Encoder.
func (j *JsonEncoder) Flush() (int64, error) {
	ret := j.w.bytesWritten
	j.w.bytesWritten = 0
	return ret, nil
}

// Read implements Encoder.
func JsonDecoderFactory(ctx context.Context, r io.Reader, factory func() any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		decoder := json.NewDecoder(r)
		dat := factory()
		lastReadBytes := decoder.InputOffset()
		for {
			if err := decoder.Decode(&dat); errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(nil, err)
				return
			}
			curBytesRead := decoder.InputOffset()
			bytesDelta := curBytesRead - lastReadBytes
			lastReadBytes = curBytesRead
			atomic.AddInt64(&stats.rowsRead, 1)
			atomic.AddInt64(&stats.bytesRead, bytesDelta)

			if !yield(dat, nil) {
				return
			}
		}
	}
}

// ReadError implements Encoder.
func (j *JsonEncoder) ReadError() error {
	return j.err
}

var _ Encoder = &JsonEncoder{}

type ioWriterCounter struct {
	io.Writer
	bytesWritten int64
}

func (i *ioWriterCounter) Write(p []byte) (n int, err error) {
	n, err = i.Writer.Write(p)
	i.bytesWritten += int64(n)
	return
}

func newJsonEncoder(ctx context.Context, w io.Writer) Encoder {
	ww := &ioWriterCounter{Writer: w}
	e := &JsonEncoder{
		w:       ww,
		Encoder: json.NewEncoder(ww),
	}
	return e
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

type PartIndex struct {
	Keys    [][]interface{}
	Offsets map[string][]int64
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

func (part *Part) WritePart(ctx context.Context, partData *PartData) (outErr error) {
	d := part.table.d
	sort.Sort(partData)
	var indicesT [][]interface{}
	def := part.table
	for _, sortDef := range def.Order {
		indicesT = append(indicesT, partData.rows[sortDef.Name])
	}

	indices := transpose(indicesT)

	granuleIndices := make([][]interface{}, 0, len(indices)/part.table.d.granuleSize)
	for i := 0; i < len(indices); i += part.table.d.granuleSize {
		granuleIndices = append(granuleIndices, indices[i])
	}

	offsets := make(map[string][]int64)
	for k, v := range partData.rows {
		file, err := d.bucket.NewWriter(ctx, part.GetPartColumnPath(k), nil)
		if err != nil {
			return fmt.Errorf("writing part %s: %w", k, err)
		}
		indexEntries, err := d.WritePartColumn(ctx, file, slices.Values(v))
		if err != nil {
			return fmt.Errorf("writing part %s: %w", k, err)
		}
		offsets[k] = indexEntries

	}
	index := &PartIndex{
		Keys:    granuleIndices,
		Offsets: offsets,
	}

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

func (t *Table) MergeParts(ctx context.Context, a, b *Part) (p *Part, outErr error) {

	p = &Part{
		id:    time.Now().UTC().Format(time.RFC3339) + uuid.New().String(),
		table: t,
	}
	var colChannels []chan interface{}
	eg, ctxGroup := errgroup.WithContext(ctx)
	pi := &PartIndex{
		Offsets: make(map[string][]int64),
	}
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
				pi.Offsets[col.Name] = indexEntries
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
				pi.Keys = append(pi.Keys, valB)
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
			pi.Keys = append(pi.Keys, valA)
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
			reader, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(o.Name), index.Offsets[o.Name][offsetIdx], -1, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			idxReaders = append(idxReaders, reader)
			decoder := part.table.d.decoderFactory(ctx, reader, func() any { return nil })
			pullers[i], stoppers[i] = iter.Pull2(decoder)

		}

		offset := index.Offsets[column][offsetIdx]
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
	i := part.table.binarySearch(idx, min, 0, len(idx.Keys)-1)
	return i, nil
}

func (t *Table) binarySearch(partIndex *PartIndex, min []interface{}, low, high int) int {
	if high <= low {
		if t.IndexCmp(partIndex.Keys[low], min) <= 0 {
			return low
		}
		return -1
	}

	mid := (low + high) / 2
	if c := t.IndexCmp(partIndex.Keys[mid], min); c < 0 {
		return t.binarySearch(partIndex, min, mid+1, high)
	} else if c > 0 {
		return t.binarySearch(partIndex, min, low, mid-1)
	} else {
		return t.binarySearch(partIndex, min, low, mid)
	}
}

func (part *Part) LoadIndex(ctx context.Context) (*PartIndex, error) {
	part.Lock()
	defer part.Unlock()
	if part.cachedIndex != nil {
		return part.cachedIndex, nil
	}
	f, err := part.table.d.bucket.NewReader(ctx, part.GetPartIndexPath(), nil)
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	ret, err := readOne2(part.table.d.decoderFactory(ctx, f, func() any { return new(PartIndex) }))
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	part.cachedIndex = ret.(*PartIndex)
	return ret.(*PartIndex), nil
}

func transpose[T any](arr [][]T) [][]T {
	if len(arr) == 0 {
		return nil
	}
	l := len(arr[0])
	w := len(arr)
	ret := make([][]T, l, l)
	for i := range ret {
		ret[i] = make([]T, w, w)
		for j := range w {
			ret[i][j] = arr[j][i]
		}
	}
	return ret
}
