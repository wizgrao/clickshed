package shed

import (
	"context"
	"testing"

	shedpb "github.com/wizgrao/clickshed/pkg/gen/shed/v1"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func newTestTable(ctx context.Context, t *testing.T, name string, granuleSize, maxGranules int32) *Table {
	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	d := &Database{
		Bucket:         b,
		EncoderFactory: NewEncoderFactory,
		DecoderFactory: ProtoDecoderFactory,
	}
	tbl, err := d.CreateTable(ctx, NewTableDef(
		name,
		[]*shedpb.Column{
			{Name: "k1", ColType: shedpb.ColType_COL_TYPE_STRING},
			{Name: "k2", ColType: shedpb.ColType_COL_TYPE_STRING},
		},
		[]*shedpb.SortDef{
			{Name: "k1", Order: shedpb.SortOrder_SORT_ORDER_ASC},
			{Name: "k2", Order: shedpb.SortOrder_SORT_ORDER_ASC},
		},
		WithGranuleSize(granuleSize),
		WithMaxGranulesPerPart(maxGranules),
	))
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func mkStrings(prefix string, n int) []string {
	vals := make([]string, n)
	for i := 0; i < n; i++ {
		vals[i] = prefix
	}
	return vals
}

func addPartWithRows(ctx context.Context, t *testing.T, table *Table, rows int) *Part {
	rowsMap := map[string]*shedpb.DatabaseValues{
		"k1": {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: mkStrings("a", rows)}}},
		"k2": {Value: &shedpb.DatabaseValues_StringValues{StringValues: &shedpb.StringValues{Values: mkStrings("b", rows)}}},
	}
	pd := table.NewPartData(rowsMap)
	p, err := table.CreatePart(ctx, pd)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTryMergeParts_ZeroParts(t *testing.T) {
	ctx := context.Background()
	table := newTestTable(ctx, t, "merge_zero", 2, 4)

	merged, err := table.TryMergeParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if merged {
		t.Fatalf("expected no merge with zero parts")
	}
}

func TestTryMergeParts_OnePart(t *testing.T) {
	ctx := context.Background()
	table := newTestTable(ctx, t, "merge_one", 2, 4)
	_ = addPartWithRows(ctx, t, table, 1)

	merged, err := table.TryMergeParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if merged {
		t.Fatalf("expected no merge with one part")
	}
}

func TestTryMergeParts_ExceedsGranuleLimit(t *testing.T) {
	ctx := context.Background()
	// GranuleSize=2, we will create two parts each with 4 rows -> 2 granules each.
	// MaxGranulesPerPart=3, so 2+2 > 3 => no merge.
	table := newTestTable(ctx, t, "merge_exceed", 2, 3)
	_ = addPartWithRows(ctx, t, table, 4) // 2 granules
	_ = addPartWithRows(ctx, t, table, 4) // 2 granules

	merged, err := table.TryMergeParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if merged {
		t.Fatalf("expected no merge due to granule limit")
	}
}

func TestTryMergeParts_MergesSmallestTwoOfThree(t *testing.T) {
	ctx := context.Background()
	// GranuleSize=2; create parts with rows: 2 (1 granule), 6 (3 granules), 4 (2 granules)
	// Smallest two are 1 and 2 granules; they should be merged.
	table := newTestTable(ctx, t, "merge_three", 2, 100)
	pSmall := addPartWithRows(ctx, t, table, 2) // 1 granule
	pMid := addPartWithRows(ctx, t, table, 4)   // 2 granules
	pLarge := addPartWithRows(ctx, t, table, 6) // 3 granules
	_ = pLarge

	merged, err := table.TryMergeParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !merged {
		t.Fatalf("expected a merge to occur")
	}

	// After merge, active parts should include the merged part and the remaining largest part.
	parts, err := table.GetActiveParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 active parts after merge, got %d", len(parts))
	}
	// Find the merged part by checking Inherits length == 2, and ensure it inherited the two smallest ids.
	var mergedPart *Part
	for _, p := range parts {
		idx, err := p.LoadIndex(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(idx.GetInherits()) == 2 {
			mergedPart = p
			break
		}
	}
	if mergedPart == nil {
		t.Fatalf("did not find merged part among active parts")
	}
	idx, err := mergedPart.LoadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inh := idx.GetInherits()
	wantA, wantB := pSmall.id, pMid.id
	if !((inh[0] == wantA && inh[1] == wantB) || (inh[0] == wantB && inh[1] == wantA)) {
		t.Fatalf("merged part did not inherit smallest two parts; inherits=%v, want ids %q and %q", inh, wantA, wantB)
	}
}
