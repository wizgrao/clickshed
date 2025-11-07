package main

import (
	"context"
	"fmt"

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
	parts, err := tbl.GetActiveParts(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(parts))
	for vals, err := range tbl.ScanColumnsRange(ctx, []any{fmt.Sprintf("%10d", 443556)},[]any{fmt.Sprintf("%10d", 443567)}, "key", "val") {
		if err != nil {
			panic(err)
		}
		fmt.Println(vals)
	}

}
