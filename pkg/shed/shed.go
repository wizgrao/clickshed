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
	"time"

	"github.com/google/uuid"
	"gocloud.dev/blob"
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
	Encode(v any) ([]byte, error)
	Read(context.Context, io.Reader, func() any) iter.Seq2[any, error]
}

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
	granuleSize int
	encoder     Encoder
	bucket      *blob.Bucket
	prefix      string
	error       error
}

func (d *Driver) Error() error {
	return d.error
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
	var indexEntries []int64
	var bytesWritten int64
	var i int64
	for element := range elements {
		if i%int64(d.granuleSize) == 0 {
			indexEntries = append(indexEntries, bytesWritten)
		}
		encoded, err := d.encoder.Encode(element)
		if err != nil {
			return nil, fmt.Errorf("writing part: %w", err)
		}

		bs, err := w.Write(encoded)
		if err != nil {
			return nil, fmt.Errorf("writing part: %w", err)
		}
		bytesWritten += int64(bs)
		i++
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
	id    string
	table *Table
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
func (p *PartData) Less(i int, j int) bool {
	for _, sortDef := range p.def.Order {

		if cmped := cmp(p.rows[sortDef.Name][i], p.rows[sortDef.Name][j]); cmped != 0 {
			return int(sortDef.Order)*cmped < 0
		}
	}
	return false
}

// Swap implements sort.Interface.
func (p *PartData) Swap(i int, j int) {

	for _, elems := range p.rows {
		elems[i], elems[j] = elems[j], elems[i]
	}
}

type JsonEncoder struct {
	Prefix string
	Indent string
	err    error
}

// Read implements Encoder.
func (j JsonEncoder) Read(ctx context.Context, r io.Reader, factory func() any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		decoder := json.NewDecoder(r)
		dat := factory()
		for {
			if err := decoder.Decode(&dat); errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(nil, err)
				return
			}
			if !yield(dat, nil) {
				return
			}
		}
	}
}

// ReadError implements Encoder.
func (j JsonEncoder) ReadError() error {
	return j.err
}

var _ Encoder = JsonEncoder{}

// Encode implements Encoder.
func (j JsonEncoder) Encode(v any) ([]byte, error) {
	if len(j.Indent)+len(j.Prefix) == 0 {
		bs, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		return append(bs, byte('\n')), nil
	}
	bs, err := json.MarshalIndent(v, j.Prefix, j.Indent)
	if err != nil {
		return nil, err
	}

	return append(bs, byte('\n')), nil
}

var _ sort.Interface = new(PartData)

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

	indexSerialized, err := d.encoder.Encode(index)
	if err != nil {
		return fmt.Errorf("writing part: %w", err)
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
	indexFile.Write(indexSerialized)

	return nil
}

func (part *Part) ScanColumn(ctx context.Context, column string, offset int64, l int64) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		f, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(column), offset, l, nil)
		defer f.Close()
		if err != nil {
			yield(nil, err)
			return
		}
		for r, e := range part.table.d.encoder.Read(ctx, f, func() interface{} { return nil }) {
			if !yield(r, e) {
				return
			}
		}
	}
}

func (part *Part) LoadIndex(ctx context.Context) (*PartIndex, error) {
	f, err := part.table.d.bucket.NewReader(ctx, part.GetPartIndexPath(), nil)
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	ret, err := readOne2(part.table.d.encoder.Read(ctx, f, func() any { return new(PartIndex) }))
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
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
