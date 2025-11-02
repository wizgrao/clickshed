package shed

import (
	"context"
	"sync/atomic"
	"testing"

	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)

func TestReadWrite(t *testing.T) {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{
		granuleSize:    2,
		bucket:         b,
		prefix:         "asdf",
		encoderFactory: NewEncoderFactory,
		decoderFactory: ProtoDecoderFactory,
	}
	table := &Table{
		d: d,
		TableDef: &shedpb.TableDef{
			Columns: []*shedpb.Column{
				&shedpb.Column{ColType: shedpb.ColType_COL_TYPE_STRING, Name: "key"},
				&shedpb.Column{ColType: shedpb.ColType_COL_TYPE_FLOAT, Name: "val"},
			},
			Order: []*shedpb.SortDef{&shedpb.SortDef{Name: "key", Order: shedpb.SortOrder_SORT_ORDER_ASC}},
		},
	}

	keys := []string{
		"asdf",
		"aaa",
		"bb",
		"zzz",
		"ddd",
	}

	values := []float64{
		float64(1.0),
		float64(2.0),
		float64(3.0),
		float64(5.0),
		float64(4.0),
	}

	rows := map[string]*shedpb.DatabaseValues{
		"key": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: keys}}},
		"val": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: values}}},
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

	valuesExpected = []interface{}{
		float64(3.0),
		float64(4.0),
	}

	ctr = 0
	_, err = part.LoadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}

	stats = IOStatistics{}
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
	if got := atomic.LoadInt64(&stats.rowsRead); got != 6 {
		t.Error("mismatched rows read, expected", 6, "got", got)
	}

	stats = IOStatistics{}
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
	if got := atomic.LoadInt64(&stats.rowsRead); got != 8 {
		t.Error("mismatched rows read, expected", 8, "got", got)
	}

}

func TestMerge(t *testing.T) {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{
		granuleSize:    2,
		bucket:         b,
		prefix:         "asdf",
		encoderFactory: NewEncoderFactory,
		decoderFactory: ProtoDecoderFactory,
	}
	table := &Table{
		d: d,
		TableDef: &shedpb.TableDef{
			Columns: []*shedpb.Column{
				&shedpb.Column{ColType: shedpb.ColType_COL_TYPE_STRING, Name: "key"},
				&shedpb.Column{ColType: shedpb.ColType_COL_TYPE_FLOAT, Name: "val"},
			},
			Order: []*shedpb.SortDef{&shedpb.SortDef{Name: "key", Order: shedpb.SortOrder_SORT_ORDER_ASC}},
		},
	}
	keys1 := []string{
		"asdf",
		"aaa",
		"bb",
		"zzz",
		"ddd",
	}

	values1 := []float64{
		float64(1.0),
		float64(2.0),
		float64(3.0),
		float64(5.0),
		float64(4.0),
	}

	rows1 := map[string]*shedpb.DatabaseValues{
		"key": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: keys1}}},
		"val": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: values1}}},
	}
	pd1 := table.NewPartData(rows1)

	part1, err := table.CreatePart(ctx, pd1)

	keys2 := []string{
		"asdg",
		"aab",
		"bc",
		"zzzz",
		"dde",
	}

	values2 := []float64{
		float64(1.5),
		float64(2.5),
		float64(3.5),
		float64(5.5),
		float64(4.5),
	}

	rows2 := map[string]*shedpb.DatabaseValues{
		"key": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: keys2}}},
		"val": &shedpb.DatabaseValues{Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: values2}}},
	}
	pd2 := table.NewPartData(rows2)

	part2, err := table.CreatePart(ctx, pd2)
	if err != nil {
		t.Fatal(err)
	}

	part, err := table.MergeParts(ctx, part1, part2)
	if err != nil {
		t.Fatal(err)
	}

	keysExpected := []string{
		"aaa",
		"aab",
		"asdf",
		"asdg",
		"bb",
		"bc",
		"ddd",
		"dde",
		"zzz",
		"zzzz",
	}

	valuesExpected := []float64{
		float64(2),
		float64(2.5),
		float64(1),
		float64(1.5),
		float64(3),
		float64(3.5),
		float64(4),
		float64(4.5),
		float64(5),
		float64(5.5),
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

	_, err = part.LoadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Verify range scan uses merged index correctly
	valuesExpectedRange := []interface{}{float64(3.0), float64(3.5), float64(4.0), float64(4.5)}
	stats = IOStatistics{}
	ctr = 0
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"bb"}, []any{"dde"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpectedRange[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr += 1
	}
	if ctr != len(valuesExpectedRange) {
		t.Error("mismatched length, expected", len(valuesExpectedRange), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 10 {
		t.Error("mismatched rows read, expected", 10, "got", got)
	}

	// Another range to exercise start-of-granule and end-boundary behavior
	// Sanity-check IndexLeq on merged index for this range
	off, err := part.IndexLeq(ctx, []any{"ab"})
	if err != nil {
		t.Fatal(err)
	}
	if off == -1 {
		idx, _ := part.LoadIndex(ctx)
		var keys []string
		if sv := idx.GetKeys()[0].GetStringValues(); sv != nil {
			keys = sv.GetValues()
		}
		t.Fatalf("IndexLeq('ab') returned -1; keys=%v", keys)
	}
	valuesExpectedRange = []interface{}{float64(1.0), float64(1.5), float64(3.0), float64(3.5)}
	stats = IOStatistics{}
	ctr = 0
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"ab"}, []any{"bc"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpectedRange[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr += 1
	}
	if ctr != len(valuesExpectedRange) {
		t.Error("mismatched length, expected", len(valuesExpectedRange), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 14 {
		t.Error("mismatched rows read, expected", 14, "got", got)
	}

	/*
		expectedIndex := &PartIndex{
			Keys: [][]interface{}{
				[]interface{}{"aaa"},
				[]interface{}{"asdf"},
				[]interface{}{"bb"},
				[]interface{}{"ddd"},
				[]interface{}{"zzz"}},
			Offsets: map[string][]int64{
				"key": []int64{0, 12, 26, 36, 48},
				"val": []int64{0, 6, 12, 18, 24},
			},
		}

		if !reflect.DeepEqual(index, expectedIndex) {
			t.Errorf("mismatched index, expected %+v, got %#v", expectedIndex, index)
		}
	*/
}

func TestMultiColumnIndex(t *testing.T) {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{
		granuleSize:    2,
		bucket:         b,
		prefix:         "asdf",
		encoderFactory: NewEncoderFactory,
		decoderFactory: ProtoDecoderFactory,
	}
	table := &Table{
		d: d,
		TableDef: &shedpb.TableDef{
			Columns: []*shedpb.Column{
				{Name: "k1", ColType: shedpb.ColType_COL_TYPE_STRING},
				{Name: "k2", ColType: shedpb.ColType_COL_TYPE_STRING},
				{Name: "val", ColType: shedpb.ColType_COL_TYPE_FLOAT},
			},
			Order: []*shedpb.SortDef{
				{Name: "k1", Order: shedpb.SortOrder_SORT_ORDER_ASC},
				{Name: "k2", Order: shedpb.SortOrder_SORT_ORDER_ASC},
			},
		},
	}

	k1 := []string{"b", "a", "b", "c", "a", "b"}
	k2 := []string{"a", "a", "d", "a", "c", "b"}
	vals := []float64{3, 1, 5, 6, 2, 4}

	rows := map[string]*shedpb.DatabaseValues{
		"k1":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k1}}},
		"k2":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k2}}},
		"val": {Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: vals}}},
	}
	pd := table.NewPartData(rows)

	part, err := table.CreatePart(ctx, pd)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure index persists and loads
	if _, err := part.LoadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	// Range 1: [ (b,a) , (b,c) ] -> values [3,4]
	valuesExpected := []interface{}{float64(3), float64(4)}
	stats = IOStatistics{}
	var ctr int
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"b", "a"}, []any{"b", "c"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpected[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(valuesExpected) {
		t.Error("mismatched length, expected", len(valuesExpected), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 9 { // 3 processed rows * (2 index cols + 1 scanned col)
		t.Error("mismatched rows read, expected", 9, "got", got)
	}

	// Range 2: [ (a,b) , (b,a) ] -> values [2,3]
	valuesExpected = []interface{}{float64(2), float64(3)}
	stats = IOStatistics{}
	ctr = 0
	for r, err := range part.ScanColumnRange(ctx, "val", []any{"a", "b"}, []any{"b", "a"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valuesExpected[ctr]; r != expected {
			t.Error("mismatched values, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(valuesExpected) {
		t.Error("mismatched length, expected", len(valuesExpected), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 12 { // 4 processed rows * 3 readers
		t.Error("mismatched rows read, expected", 12, "got", got)
	}
}

func TestMergeMultiColumnIndex(t *testing.T) {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Driver{
		granuleSize:    2,
		bucket:         b,
		prefix:         "asdf",
		encoderFactory: NewEncoderFactory,
		decoderFactory: ProtoDecoderFactory,
	}
	table := &Table{
		d: d,
		TableDef: &shedpb.TableDef{
			Columns: []*shedpb.Column{
				{Name: "k1", ColType: shedpb.ColType_COL_TYPE_STRING},
				{Name: "k2", ColType: shedpb.ColType_COL_TYPE_STRING},
				{Name: "val", ColType: shedpb.ColType_COL_TYPE_FLOAT},
			},
			Order: []*shedpb.SortDef{
				{Name: "k1", Order: shedpb.SortOrder_SORT_ORDER_ASC},
				{Name: "k2", Order: shedpb.SortOrder_SORT_ORDER_ASC},
			},
		},
	}

	k1a := []string{"a", "b", "b", "c"}
	k2a := []string{"a", "a", "d", "a"}
	valsA := []float64{1, 3, 7, 6}

	rowsA := map[string]*shedpb.DatabaseValues{
		"k1":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k1a}}},
		"k2":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k2a}}},
		"val": {Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: valsA}}},
	}
	pdA := table.NewPartData(rowsA)
	partA, err := table.CreatePart(ctx, pdA)
	if err != nil {
		t.Fatal(err)
	}

	k1b := []string{"a", "b", "b", "c"}
	k2b := []string{"b", "b", "c", "b"}
	valsB := []float64{2, 4, 5, 8}
	rowsB := map[string]*shedpb.DatabaseValues{
		"k1":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k1b}}},
		"k2":  {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: k2b}}},
		"val": {Value: &shedpb.DatabaseValues_FloatValues{FloatValues: &shedpb.FloatValues{Values: valsB}}},
	}
	pdB := table.NewPartData(rowsB)
	partB, err := table.CreatePart(ctx, pdB)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := table.MergeParts(ctx, partA, partB)
	if err != nil {
		t.Fatal(err)
	}

	// Validate merged order via scanning keys and values
	k1Expected := []interface{}{"a", "a", "b", "b", "b", "b", "c", "c"}
	k2Expected := []interface{}{"a", "b", "a", "b", "c", "d", "a", "b"}
	valsExpected := []interface{}{float64(1), float64(2), float64(3), float64(4), float64(5), float64(7), float64(6), float64(8)}

	var ctr int
	for r, err := range merged.ScanColumn(ctx, "k1", 0, -1) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := k1Expected[ctr]; r != expected {
			t.Error("mismatched k1, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(k1Expected) {
		t.Error("mismatched k1 length, expected", len(k1Expected), "got", ctr)
	}

	ctr = 0
	for r, err := range merged.ScanColumn(ctx, "k2", 0, -1) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := k2Expected[ctr]; r != expected {
			t.Error("mismatched k2, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(k2Expected) {
		t.Error("mismatched k2 length, expected", len(k2Expected), "got", ctr)
	}

	ctr = 0
	for r, err := range merged.ScanColumn(ctx, "val", 0, -1) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valsExpected[ctr]; r != expected {
			t.Error("mismatched vals, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(valsExpected) {
		t.Error("mismatched vals length, expected", len(valsExpected), "got", ctr)
	}

	if _, err := merged.LoadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	// Range 1: [ (b,a) , (b,c) ] -> values [3,4,5]; rowsRead = 9
	stats = IOStatistics{}
	valsExpectedRange := []interface{}{float64(3), float64(4), float64(5)}
	ctr = 0
	for r, err := range merged.ScanColumnRange(ctx, "val", []any{"b", "a"}, []any{"b", "c"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valsExpectedRange[ctr]; r != expected {
			t.Error("mismatched range vals, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(valsExpectedRange) {
		t.Error("mismatched range length, expected", len(valsExpectedRange), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 12 {
		t.Error("mismatched rows read, expected", 12, "got", got)
	}

	// Range 2: [ (a,b) , (b,a) ] -> values [2,3]; processed rows 4 => rowsRead 12
	stats = IOStatistics{}
	valsExpectedRange = []interface{}{float64(2), float64(3)}
	ctr = 0
	for r, err := range merged.ScanColumnRange(ctx, "val", []any{"a", "b"}, []any{"b", "a"}) {
		if err != nil {
			t.Fatal(err)
		}
		if expected := valsExpectedRange[ctr]; r != expected {
			t.Error("mismatched range vals, expected", expected, "got", r)
		}
		ctr++
	}
	if ctr != len(valsExpectedRange) {
		t.Error("mismatched range length, expected", len(valsExpectedRange), "got", ctr)
	}
	if got := atomic.LoadInt64(&stats.rowsRead); got != 12 {
		t.Error("mismatched rows read, expected", 12, "got", got)
	}
}
