package main

import (
	"context"
	"fmt"

	shedv1 "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"github.com/wizgrao/clickshed/pkg/shed"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
)

func main() {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, "gs://gauravrao")
	if err != nil {
		panic(err)
	}
	db := &shed.Database{
		Bucket:         blob.PrefixedBucket(b, "test/"),
		EncoderFactory: shed.NewEncoderFactory,
		DecoderFactory: shed.ProtoDecoderFactory,
	}
	tbl, err := db.OpenTable(ctx, "testPlusOne")
	partSize := 100000
	for j := range 100 {
		var strKeys []string
		var valFloats []float64
		for i := range partSize {
			ii := j*partSize + i
			strKeys = append(strKeys, fmt.Sprintf("%10d", ii))
			valFloats = append(valFloats, float64(ii+1))
		}

		ks := &shedv1.DatabaseValues{
			Value: &shedv1.DatabaseValues_StringValues{
				StringValues: &shedv1.StringValues{
					Values: strKeys,
				},
			},
		}

		vals := &shedv1.DatabaseValues{
			Value: &shedv1.DatabaseValues_FloatValues{
				FloatValues: &shedv1.FloatValues{
					Values: valFloats,
				},
			},
		}
		_, err := tbl.CreatePart(ctx, tbl.NewPartData(map[string]*shedv1.DatabaseValues{"key": ks, "val": vals}))
		if err != nil {
			panic(err)
		}
	}

}
