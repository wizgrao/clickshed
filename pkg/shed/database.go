package shed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"

	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
)

type Database struct {
	granuleSize    int
	encoderFactory EncoderFactory
	decoderFactory DecoderFactory
	bucket         *blob.Bucket
}

var TableAlreadyExists = errors.New("table already exists")

func (d *Database) CreateTable(ctx context.Context, td *shedpb.TableDef) (*Table, error) {
	specPath := path.Join("table", td.Name, "spec")
	exists, err := d.bucket.Exists(ctx, specPath)
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	if exists {
		return nil, TableAlreadyExists
	}

	writer, err := d.bucket.NewWriter(ctx, specPath, nil)
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	defer writer.Close()

	encoder := d.encoderFactory(ctx, writer)
	ts := &shedpb.TableState{
		Def: td,
	}
	if err := encoder.Encode(ts); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	if _, err := encoder.Flush(); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	return &Table{
		d:          d,
		TableState: ts,
	}, nil
}

func (d *Database) OpenTable(ctx context.Context, tableName string) (*Table, error) {
	specPath := path.Join("table", tableName, "spec")
	r, err := d.bucket.NewReader(ctx, specPath, nil)
	if err != nil {
		return nil, fmt.Errorf("open table %s: %w", tableName, err)
	}
	defer r.Close()
	msg, err := readOne2(d.decoderFactory(ctx, r, func() any { return new(shedpb.TableState) }))
	if err != nil {
		return nil, fmt.Errorf("open table %s: %w", tableName, err)
	}
	return &Table{d: d, TableState: msg.(*shedpb.TableState)}, nil
}

func (d *Database) ListTables(ctx context.Context) ([]*Table, error) {
	var tables []*Table
	iter := d.bucket.List(&blob.ListOptions{Prefix: path.Join("table")})
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
func (d *Database) WritePartColumn(ctx context.Context, w io.Writer, elements iter.Seq[interface{}]) (offsets []int64, err error) {
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
