package shed

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"path"
	"sort"
	"sync"

	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
)

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

// Less implements sort.Interface.
func (p *PartData) Less(i, j int) bool {
	for _, sortDef := range p.Def.Order {
		if cmped := cmp(indexIntoDbValues(p.Rows[sortDef.Name], i), indexIntoDbValues(p.Rows[sortDef.Name], j)); cmped != 0 {
			return int(sortDef.Order)*cmped < 0
		}
	}
	return false
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

func (part *Part) GetPartColumnPath(k string) string {
	return path.Join("column", part.table.Def.GetName(), part.id, k+".col")
}

func (part *Part) GetPartIndexPath() string {
	return path.Join("index", part.table.Def.GetName(), part.id+".idx")
}

func (part *Part) WritePart(ctx context.Context, partData *PartData) (outErr error) {
	d := part.table.d
	sort.Sort(partData)
	var indicesT []*shedpb.DatabaseValues

	def := part.table.Def
	for _, sortDef := range def.Order {
		indicesT = append(indicesT, partData.Rows[sortDef.Name])
	}
	index := initPartIndex(part.id, part.table.Def, nil)
	partLen := lenDbVals(indicesT[0])

	for i := 0; i < partLen; i += int(part.table.Def.GranuleSize) {
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
		indexEntries, err := part.table.WritePartColumn(ctx, file, AllDbVals(v))
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

func (part *Part) IndexColumnsIterators(ctx context.Context) (iter.Seq2[[]any, error], iter.Seq2[[]any, error]) {
	var idxs []iter.Seq2[any, error]
	for _, o := range part.table.Def.Order {
		idxs = append(idxs, part.ScanColumn(ctx, o.Name, 0, -1))
	}
	var rets []iter.Seq2[any, error]

	for _, col := range part.table.Def.Columns {
		rets = append(rets, part.ScanColumn(ctx, col.Name, 0, -1))
	}

	return ZipIters(idxs), ZipIters(rets)
}

func (part *Part) ScanColumnRange(ctx context.Context, column string, minIndex, maxIndex []interface{}) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		for rs, err := range part.ScanColumnsRange(ctx, minIndex, maxIndex, column) {
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(rs[len(part.table.Def.Order)], nil) {
				return
			}
		}
	}
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

func (part *Part) ScanColumnsRange(ctx context.Context, minIndex, maxIndex []interface{}, columns ...string) iter.Seq2[[]any, error] {
	return func(yield func([]any, error) bool) {
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

		numIndices := len(part.table.Def.Order)
		numRequested := len(columns)
		numCols := numIndices + numRequested

		var idxReaders []*blob.Reader
		decoders := make([]iter.Seq2[any, error], numCols)

		for i, o := range part.table.Def.Order {
			reader, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(o.Name), index.GetOffsets()[o.Name].GetOffsets()[offsetIdx], -1, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			idxReaders = append(idxReaders, reader)
			decoders[i] = part.table.d.decoderFactory(ctx, reader, func() any { return nil })

		}

		for i, o := range columns {
			reader, err := part.table.d.bucket.NewRangeReader(ctx, part.GetPartColumnPath(o), index.GetOffsets()[o].GetOffsets()[offsetIdx], -1, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			idxReaders = append(idxReaders, reader)
			decoders[i+numIndices] = part.table.d.decoderFactory(ctx, reader, func() any { return nil })

		}

		for rs, e := range ZipIters(decoders) {
			idxElems := rs[:numIndices]

			if minIndex != nil && part.table.IndexCmp(minIndex, idxElems) > 0 {
				continue
			}
			if maxIndex != nil && part.table.IndexCmp(maxIndex, idxElems) < 0 {
				return
			}

			if !yield(rs, e) {
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
