package shed

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"iter"
	"sync/atomic"
)

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

var _ Encoder = &ProtobufEncoder{}

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
