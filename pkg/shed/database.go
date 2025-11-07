package shed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"sync"
	"time"

	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
)

type Database struct {
	EncoderFactory EncoderFactory
	DecoderFactory DecoderFactory
	Bucket         *blob.Bucket

	sync.RWMutex
	Cache map[string]*shedpb.PartIndex
}

func (d *Database) CheckCache(path string) *shedpb.PartIndex {
	d.RLock()
	defer d.RUnlock()
	return d.Cache[path]
}
func (d *Database) LoadIndex(ctx context.Context, path string) (*shedpb.PartIndex, error) {
	if val := d.CheckCache(path); val != nil {
		return val, nil
	}
	f, err := d.Bucket.NewReader(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	defer f.Close()
	ret, err := readOne2(d.DecoderFactory(ctx, f, func() any { return new(shedpb.PartIndex) }))
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	retVal := ret.(*shedpb.PartIndex)
	d.Lock()
	defer d.Unlock()
	if d.Cache == nil {
		d.Cache = make(map[string]*shedpb.PartIndex)
	}
	d.Cache[path] = retVal
	return retVal, nil
}

var TableAlreadyExists = errors.New("table already exists")

func (d *Database) CreateTable(ctx context.Context, td *shedpb.TableDef) (*Table, error) {
	specPath := path.Join("table", td.Name, "spec")
	exists, err := d.Bucket.Exists(ctx, specPath)
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	if exists {
		return nil, TableAlreadyExists
	}

	writer, err := d.Bucket.NewWriter(ctx, specPath, nil)
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	defer writer.Close()

	encoder := d.EncoderFactory(ctx, writer)
	if err := encoder.Encode(td); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	if _, err := encoder.Flush(); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	return &Table{
		d:   d,
		Def: td,
	}, nil
}

func (d *Database) MergeLoop(ctx context.Context) error {
	hasLoop := make(map[string]bool)
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		for {
			tables, err := d.ListTables(ctx)
			if err != nil {
				return err
			}
			for _, table := range tables {
				if hasLoop[table.Def.GetName()] {
					continue
				}
				hasLoop[table.Def.GetName()] = true
				errGroup.Go(func() error {
					return table.MergeLoop(ctx)
				})
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	})
	return errGroup.Wait()

}

func (d *Database) OpenTable(ctx context.Context, tableName string) (*Table, error) {
	specPath := path.Join("table", tableName, "spec")
	r, err := d.Bucket.NewReader(ctx, specPath, nil)
	if err != nil {
		return nil, fmt.Errorf("open table %s: %w", tableName, err)
	}
	defer r.Close()
	msg, err := readOne2(d.DecoderFactory(ctx, r, func() any { return new(shedpb.TableDef) }))
	if err != nil {
		return nil, fmt.Errorf("open table %s: %w", tableName, err)
	}
	return &Table{d: d, Def: msg.(*shedpb.TableDef)}, nil
}

func (d *Database) ListTables(ctx context.Context) ([]*Table, error) {
	var tables []*Table
	iter := d.Bucket.List(&blob.ListOptions{Prefix: path.Join("table")})
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if path.Base(obj.Key) != "spec" {
			continue
		}
		_, parent := path.Split(path.Dir(obj.Key))
		t, err := d.OpenTable(ctx, parent)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}
func (table *Table) WritePartColumn(ctx context.Context, w io.Writer, elements iter.Seq[interface{}]) (offsets []int64, err error) {
	d := table.d
	defer func() {
		if wc, ok := w.(io.WriteCloser); ok {
			if err2 := wc.Close(); err2 != nil {
				err = errors.Join(err, err2)
			}
		}
	}()
	encoder := d.EncoderFactory(ctx, w)
	var indexEntries []int64
	var bytesWritten int64
	var i int64
	for element := range elements {
		if i%int64(table.Def.GetGranuleSize()) == 0 {
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
