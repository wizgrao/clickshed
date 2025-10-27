package shed

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)

func TestWrite(t *testing.T) {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{
		granuleSize: 2,
		encoder:     &JsonEncoder{},
		bucket:      b,
		prefix:      "asdf",
	}
	table := &Table{
		d: d,
		Columns: []Column{
			Column{T: StringType, Name: "key"},
			Column{T: FloatType, Name: "val"},
		},
		Order: []SortDef{SortDef{Name: "key", Order: SortOrderAsc}},
	}

	keys := []interface{}{
		"asdf",
		"aaa",
		"bb",
		"zzz",
		"ddd",
	}

	values := []interface{}{
		float64(1.0),
		float64(2.0),
		float64(3.0),
		float64(5.0),
		float64(4.0),
	}

	rows := map[string][]interface{}{
		"key": keys,
		"val": values,
	}
	pd := table.NewPartData(rows)

	part, err := table.CreatePart(ctx, pd)

	if err != nil {
		t.Fatal(err)
	}

	keysExpected := []interface{}{
		"aaa",
		"asdf",
		"bb",
		"ddd",
		"zzz",
	}

	valuesExpected := []interface{}{
		float64(2.0),
		float64(1.0),
		float64(3.0),
		float64(4.0),
		float64(5.0),
	}

	var ctr int
	for r, err := range part.ScanColumn(ctx, "key", 0, -1) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := keysExpected[ctr]; r != expected {
			t.Error("mismatched keys, expected", expected, "got", r)
		}

		ctr += 1
	}
	if ctr != len(keysExpected) {
		t.Error("length of scanned column, expected", len(keysExpected), "got", ctr)
	}

	ctr = 0

	for r, err := range part.ScanColumn(ctx, "val", 0, -1) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpected[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr += 1
	}
	if ctr != len(keysExpected) {
		t.Error("length of scanned column, expected", len(keysExpected), "got", ctr)
	}

	index, err := part.LoadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}

	expectedIndex := &PartIndex{
		Keys: [][]interface{}{{"aaa"}, {"bb"}, {"zzz"}},

		Offsets: map[string][]int64{
			"key": {0, 13, 24},
			"val": {0, 4, 8},
		},
	}

	if !reflect.DeepEqual(index, expectedIndex) {
		t.Errorf("mismatched index, expected %+v, got %+v", expectedIndex, index)
	}

	valuesExpected = []interface{}{
		float64(3.0),
		float64(4.0),
	}

	ctr = 0

	enc := &JsonEncoder{}
	d.encoder = enc // reset statistics
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"bb"}, []any{"ddd"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpected[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr += 1
	}
	if ctr != len(valuesExpected) {
		t.Error("mismatched length, expected", len(valuesExpected), "got", ctr)
	}
	if got := atomic.LoadInt64(&enc.stats.rowsRead); got != 6 {
		t.Error("mismatched rows read, expected", 6, "got", got)
	}

	enc.stats = IOStatistics{}
	valuesExpected = []interface{}{
		float64(1.0), float64(3.0),
	}
	ctr = 0
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"ab"}, []any{"bc"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpected[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr += 1
	}
	if ctr != len(valuesExpected) {
		t.Error("mismatched length, expected", len(valuesExpected), "got", ctr)
	}
	if got := atomic.LoadInt64(&enc.stats.rowsRead); got != 8 {
		t.Error("mismatched rows read, expected", 8, "got", got)
	}

}
