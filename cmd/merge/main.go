package main

import (
	"context"
	"errors"

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
	_, err = db.CreateTable(ctx, shed.NewTableDef(
		"testPlusOne",
		[]*shedv1.Column{
			&shedv1.Column{Name: "key", ColType: shedv1.ColType_COL_TYPE_STRING},
			&shedv1.Column{Name: "val", ColType: shedv1.ColType_COL_TYPE_FLOAT}},
		[]*shedv1.SortDef{
			&shedv1.SortDef{Name: "key", Order: shedv1.SortOrder_SORT_ORDER_ASC},
		},
	))
	if err != nil && !errors.Is(err, shed.TableAlreadyExists) {
		panic(err)
	}
	if err := db.MergeLoop(ctx); err != nil {
		panic(err)
	}
}
