# Clickshed MergeTree: Architecture & Guide

This repository implements a lightweight, columnar MergeTree-like storage over gocloud’s blob abstraction, with protobuf-based serialization.

- Columnar parts are written to a blob bucket (local, memory, cloud providers via gocloud.dev).
- Each part has a compact index that stores per-granule keys and byte offsets into column files.
- Parts can be merged into new parts; merged parts record ancestry so older parts can be ignored.


## MergeTree Overview

- Table definition (`shed.v1.TableDef`) sets columns, sort order (`Order`), and `GranuleSize`.
- A Part is an immutable, sorted, columnar chunk of a table.
  - Data is sorted by the multi-column sort key when writing a part.
  - Every `GranuleSize` rows, the part records:
    - First-row key for each sort column (into the index `keys` array).
    - Current byte offset in each column file (into index `offsets`).
- Table scans across all active parts perform a k-way merge by sort key.
- Merging two parts produces a new, sorted part; the new part’s index includes `inherits` with the parent part IDs.

Key code:
- `pkg/shed/part.go` (write parts, scan a part, binary search index).
- `pkg/shed/table.go` (merge parts, table-wide scans, active parts discovery).
- `pkg/shed/shed.go` (encoder/decoder, iter utilities, comparisons).
- `shed/v1/table.proto` (protobuf schema).


## Bucket Layout (blob paths)

All objects live under three prefixes per table:

- Table spec (schema): `table/<table_name>/spec` — encoded `TableDef`.
- Part columns: `column/<table_name>/<part_id>/<column>.col` — framed protobuf stream of column values.
- Part index: `index/<table_name>/<part_id>.idx` — encoded `PartIndex` with:
  - `keys`: for each sort column, the first key of each granule.
  - `offsets`: per column, the byte offsets of each granule boundary in its `.col` file.
  - `inherits`: parent part IDs if created by a merge.
  - `created`, `id` metadata.

Active parts = all parts that are not listed as an `inherits` of another part (see `Table.GetActiveParts`).


## Serialization Format

Transport is length-delimited protobuf frames written to blob objects.

- Writer: `ProtobufEncoder` frames messages as `[uvarint length][bytes]`.
- Column data is buffered as `shed.v1.DatabaseValues` (oneof of `StringValues` or `FloatValues`).
  - `Encode` appends scalar values to a `DatabaseValues` batch.
  - `Flush` writes the batched `DatabaseValues` (and any queued proto messages) in framed form and resets buffers.
- `WritePartColumn` streams elements and:
  - Calls `Flush` at every granule boundary to close a batch, accumulating `bytesWritten`.
  - Records the cumulative `bytesWritten` after each flush into the part index’s `offsets` for that column.
- Reader: `ProtoDecoderFactory` reads `[uvarint length]` then decodes:
  - If a `proto.Message` factory is provided, yields whole messages (e.g., `PartIndex`).
  - Otherwise, decodes `DatabaseValues` frames and yields individual scalars from the frame.

This yields compact column files with chunk boundaries aligned to granules for efficient range reads.


## Scans and Merges

- Part range scan (`Part.ScanColumnsRange`):
  - Loads `PartIndex`, binary-searches `keys` to find the starting granule (`IndexLeq`).
  - Opens range readers from recorded `offsets` for the required columns.
  - Zips iterators of index columns + requested columns; filters rows by `[min, max]` key range.
- Table range scan (`Table.ScanColumnsRange`):
  - Gets active parts, starts a scanner on each, maintains current rows, and repeatedly yields the smallest key by `IndexCmp`.
- Merge (`Table.MergeParts`):
  - Opens index and column iterators from both parts.
  - Performs a two-way merge on the sort key, fan-out to per-column writers via channels.
  - Builds a new `PartIndex` with `keys`, `offsets`, `inherits=[a.id,b.id]`, then writes new `.idx`.


## Running Tests

Requirements: Go and `gocloud.dev` drivers (brought in by tests via blank imports).

- Run all tests:
  - `go test ./...`
- Run just the storage tests:
  - `go test ./pkg/shed -v`

Tests use the in-memory bucket URL `mem://` via `blob.OpenBucket`, so no external services are required.


## Regenerate Protobufs (buf generate)

This repo uses Buf v2 with managed mode and local plugins to output Go code under `pkg/gen`.

Prereqs (once):
- Install Buf CLI: macOS Homebrew: `brew install bufbuild/buf/buf` or see https://buf.build/docs/installation
- Install protoc plugins on PATH:
  - `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
  - `go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest`

Generate from repo root:
- `buf generate`

Configuration:
- `buf.yaml` (module, lint/breaking checks)
- `buf.gen.yaml` (plugins → `protoc-gen-go`, `protoc-gen-connect-go`, output to `pkg/gen`, `go_package_prefix: github.com/wizgrao/clickshed/pkg/gen`).


## Minimal Example

- Define a table:
  - `Database.CreateTable(ctx, &TableDef{Name: "users", Columns: [...], Order: [...], GranuleSize: 2})`
- Create a part from columnar arrays: `Table.NewPartData(rows)` → `Table.CreatePart`.
- Scan a column: `Part.ScanColumn(ctx, "val", 0, -1)`.
- Range scan across parts: `Table.ScanColumnsRange(ctx, []any{"a"}, []any{"z"}, "val")`.
- Merge two parts: `Table.MergeParts(ctx, a, b)`.


## Notes

- Only `string` and `float64` columns are implemented (`COL_TYPE_STRING`, `COL_TYPE_FLOAT`).
- `GranuleSize` controls index sampling rate and chunk size. Larger granules reduce index size but may read more data on narrow ranges.
- Blob backend is pluggable via gocloud.dev (`mem://`, `file://`, `gs://`, `s3://`, etc.).
