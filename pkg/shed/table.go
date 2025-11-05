package shed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Table struct {
	d   *Database
	Def *shedpb.TableDef
}

type TableOpt func(*shedpb.TableDef)

func WithGranuleSize(granuleSize int32) TableOpt {
	return func(td *shedpb.TableDef) {
		td.GranuleSize = granuleSize
	}
}

func WithMaxGranulesPerPart(maxGranulesPerPart int32) TableOpt {
	return func(td *shedpb.TableDef) {
		td.MaxGranulesPerPart = maxGranulesPerPart
	}
}

func NewTableDef(name string, columns []*shedpb.Column, order []*shedpb.SortDef, opts ...TableOpt) *shedpb.TableDef {
	def := &shedpb.TableDef{
		Name:               name,
		Columns:            columns,
		Order:              order,
		MaxGranulesPerPart: 8192,
		GranuleSize:        8192,
	}
	for _, opt := range opts {
		opt(def)
	}
	return def
}

func (t *Table) NewPartData(rows map[string]*shedpb.DatabaseValues) *PartData {
	return &PartData{
		Def:  t.Def,
		Rows: rows,
	}
}

func (t *Table) TryMergeParts(ctx context.Context) (bool, error) {
	parts, err := t.GetActiveParts(ctx)
	if err != nil {
		return false, fmt.Errorf("try merge parts: %w", err)
	}

	if len(parts) < 2 {
		return false, nil
	}

	slices.SortFunc(parts, func(a, b *Part) int {
		a.Lock()
		aI := a.cachedIndex
		a.Unlock()
		b.Lock()
		bI := b.cachedIndex
		b.Unlock()
		return lenDbVals(aI.Keys[0]) - lenDbVals(bI.Keys[0])
	})

	a := parts[0]
	b := parts[1]
	a.Lock()
	aI := a.cachedIndex
	a.Unlock()
	b.Lock()
	bI := b.cachedIndex
	b.Unlock()

	if lenDbVals(aI.Keys[0])+lenDbVals(bI.Keys[1]) > int(t.Def.GetMaxGranulesPerPart()) {
		return false, nil
	}

	_, err = t.MergeParts(ctx, a, b)
	if err != nil {
		return false, fmt.Errorf("try merge parts: %w", err)
	}
	return true, nil
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

func (t *Table) IndexCmp(a, b []interface{}) int {
	for i, sortDef := range t.Def.Order {
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

func colMap(def *shedpb.TableDef) map[string]shedpb.ColType {
	ret := make(map[string]shedpb.ColType)
	for _, col := range def.GetColumns() {
		ret[col.GetName()] = col.GetColType()
	}
	return ret
}

func initPartIndex(id string, def *shedpb.TableDef, inherits []string) *shedpb.PartIndex {
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
		Id:       id,
		Inherits: inherits,
		Created:  timestamppb.Now(),
		Keys:     keys,
		Offsets:  make(map[string]*shedpb.Offsets),
	}
}

func (t *Table) MergeParts(ctx context.Context, a, b *Part) (p *Part, outErr error) {
	p = &Part{
		id:    time.Now().UTC().Format(time.RFC3339) + uuid.New().String(),
		table: t,
	}
	var colChannels []chan interface{}
	eg, ctxGroup := errgroup.WithContext(ctx)
	pi := initPartIndex(p.id, t.Def, []string{a.id, b.id})

	var piLock sync.Mutex
	for _, col := range t.Def.Columns {
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
				indexEntries, err := t.WritePartColumn(ctxGroup, file, func(yield func(interface{}) bool) {
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
			if ctr%int(t.Def.GranuleSize) == 0 {
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
		if ctr%int(t.Def.GranuleSize) == 0 {
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

func (table *Table) GetActiveParts(ctx context.Context) ([]*Part, error) {
	partsMap := make(map[string]*Part)
	listIter := table.d.bucket.List(&blob.ListOptions{
		Prefix: path.Join("index", table.Def.GetName()),
	})
	iterBlob, err := listIter.Next(ctx)
	for err == nil {
		_, key := path.Split(iterBlob.Key)
		partId := strings.TrimSuffix(key, ".idx")
		if _, ok := partsMap[partId]; !ok {
			part := table.OpenPart(ctx, partId)
			partsMap[partId] = part
			idx, err := part.LoadIndex(ctx)
			if err != nil {
				return nil, err
			}

			for _, fnd := range idx.Inherits {
				partsMap[fnd] = nil
			}
		}
		iterBlob, err = listIter.Next(ctx)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	var ret []*Part
	for _, part := range partsMap {
		if part != nil {
			ret = append(ret, part)
		}
	}
	return ret, nil
}

func (table *Table) ScanColumnsRange(ctx context.Context, minIndex, maxIndex []interface{}, columns ...string) iter.Seq2[[]any, error] {
	return func(yield func([]any, error) bool) {
		parts, err := table.GetActiveParts(ctx)
		if err != nil {
			yield(nil, err)
			return

		}
		numParts := len(parts)
		pullers := make([]func() ([]any, error, bool), numParts)
		curVals := make([][]any, numParts)
		stoppers := make([]func(), numParts)
		defer func() {
			for _, stopper := range stoppers {
				stopper()
			}
		}()

		for i, part := range parts {
			scanner := part.ScanColumnsRange(ctx, minIndex, maxIndex, columns...)
			pullers[i], stoppers[i] = iter.Pull2(scanner)
			curVal, err, _ := pullers[i]()
			if err != nil {
				yield(nil, err)
				return
			}
			curVals[i] = curVal
		}

		for {
			var minVal []any
			var minIdx int
			var err error

			for i, curVal := range curVals {
				if curVal == nil {
					continue
				}
				if (curVal != nil && minVal == nil) || table.IndexCmp(curVal, minVal) < 0 {
					minIdx = i
					minVal = curVal
					continue
				}
			}
			if minVal == nil {
				return
			}

			yield(minVal, nil)
			curVals[minIdx], err, _ = pullers[minIdx]()
			if err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

func (t *Table) OpenPart(ctx context.Context, id string) *Part {
	return &Part{
		id:    id,
		table: t,
	}
}
