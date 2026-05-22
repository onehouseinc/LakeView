# Iceberg Table Support

LakeView discovers and uploads metadata for Apache Iceberg tables alongside Hudi tables. For
Iceberg, the extractor uploads the current `metadata.json` pointer file each iteration; the control
plane parses the snapshot summary from it (cumulative `total-records`, `total-files-size`,
`total-data-files`, per-snapshot deltas, and the `snapshot-log[]` history) and writes metrics to
OpenSearch.

## Table format detection

Format detection is a pluggable SPI, `TableFormatDetector`, with one implementation per format:

- `HudiTableFormatDetector` — a directory is a Hudi table root if it contains a `.hoodie/` folder.
- `IcebergTableFormatDetector` — a directory is an Iceberg table root if it contains a `metadata/`
  sub-directory **and** that sub-directory holds at least one `*.metadata.json` pointer file. The
  pointer-file check separates a real Iceberg table from any folder that merely has a sub-directory
  named `metadata` (Spark checkpoint dirs, docs folders, etc.). This costs one extra `LIST` per
  candidate directory during discovery — not per upload cycle.

Detectors are **not raced** against each other. Each `Database` in the parser YAML declares its
`tableFormat`, and `TableDiscoveryService` selects the single matching detector for every directory
under that database's base paths.

### Declaring the format in parser YAML

```yaml
parserConfig:
  - lake: my_lake
    databases:
      - name: my_iceberg_db
        tableFormat: ICEBERG        # absent / null => HUDI (back-compat)
        basePaths:
          - s3://bucket/warehouse/my_iceberg_db/
```

A mixed-format warehouse should be split into separate `Database` entries, one per format.

## Selecting the current `metadata.json`

`IcebergMetadataUploaderService` resolves the current pointer through three paths, in order of
preference:

1. **`metadataLocationHint`** — the URI of the current `metadata.json` supplied by the control plane
   in the parser YAML's `tableHints` (e.g. from AWS Glue's `metadata_location` parameter). When
   present, the extractor PUTs the file directly and skips the per-cycle `metadata/` listing.
   *Hints only apply to base paths pinned with an explicit `#tableId`; auto-discovered tables always
   fall back to listing.*
2. **`metadata/version-hint.text`** — for Hadoop-catalog tables, its integer content names the
   current `v{N}.metadata.json` unambiguously.
3. **Numeric-aware filename comparison** (last resort) — the leading integer in the filename (after
   an optional `v` prefix) is the version number. Works for both `v{N}.metadata.json` (Hadoop) and
   `00000-<uuid>.metadata.json` (Hive/Glue/Spark). **Caveat:** the highest-versioned file is not
   guaranteed to be the catalog's committed pointer — a failed or concurrent write can leave an
   orphan with a higher version. This is best-effort; paths 1 and 2 are authoritative and preferred
   for exactly that reason.

The checkpoint tracks the last-uploaded filename; if it hasn't advanced since the last run, the
iteration is a no-op.

`s3a://` URIs (the Hadoop scheme that Glue/Onehouse-agent `metadata_location` values use) are
accepted by the storage URI parser in addition to `s3://`.

## Upload orchestration

`TableDiscoveryAndUploadJob.dispatchUpload` partitions discovered tables by format and runs the Hudi
(`TableMetadataUploaderService`) and Iceberg (`IcebergMetadataUploaderService`) uploaders
concurrently. A `null` table format is treated as `HUDI`. `IcebergMetadataUploaderService` is a
sibling of the Hudi uploader rather than a subclass — Hudi's active/archived timeline distinction,
`hoodie.properties` bootstrap, and LSM manifest plumbing don't apply to Iceberg — but it reuses the
shared `OnehouseApiClient`, `PresignedUrlFileUploader`, `AsyncStorageClient`, and `StorageUtils`.

On the init RPC (`InitializeTableMetricsCheckpoint`), Iceberg tables send `tableFormat=ICEBERG` and
a placeholder `tableType=COPY_ON_WRITE` (the server discriminates on `tableFormat` first; `tableType`
is meaningless for Iceberg but stays non-null to preserve the wire contract).

## Deployment ordering

⚠️ This feature spans three repositories and **must be rolled out in order**:

1. **idls** — adds the `TableFormat` proto enum / `TableMetricsCheckpoint` field
   ([idls PR #1939](https://github.com/onehouseinc/idls/pull/1939)).
2. **gateway-controller** — adds `IcebergCommitMetadataParser` to parse the uploaded `metadata.json`
   and emit metrics to OpenSearch.
3. **LakeView** (this repo) — discovers Iceberg tables and uploads `metadata.json`.

If LakeView ships **before** the server understands `tableFormat`, the server-side protobuf JSON
parser drops the unknown enum value and persists the proto enum-zero default
(`TABLE_FORMAT_INVALID`), routing Iceberg uploads through the Hudi back-compat fallback — i.e. the
control plane tries to parse an Iceberg `metadata.json` as a Hudi commit. **Before deploying
LakeView, confirm the running control plane tolerates `tableFormat=ICEBERG` cleanly** (errors or
ignores rather than corrupting the checkpoint), and deploy the idls + gateway-controller changes
first.
