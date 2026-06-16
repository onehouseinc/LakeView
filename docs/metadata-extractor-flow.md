# LakeView Metadata Extractor — End-to-End Code Flow

This document is the canonical walk-through of the **LakeView metadata-extractor** job that runs inside the customer data plane (the **Push Model** of LakeView). It is intended for engineers extending the extractor, on-callers debugging it, and AI agents that need a structured map of the codebase.

All file paths are relative to `lakeview/src/main/java/`. Where useful, line numbers are at HEAD `3275c3f` and may drift over time — treat them as orientation, not a contract.

## 1. What the extractor does

The extractor runs as a long-lived pod (or CLI invocation) inside the customer's data plane. Every tick it:

1. **Discovers** Hudi and Iceberg tables under the configured base paths in customer-owned object storage (S3, GCS, or Azure Blob).
2. **Reads** each table's metadata files — `.hoodie/` for Hudi, `metadata/` for Iceberg.
3. **Fetches** pre-signed PUT URLs from the Onehouse control-plane API.
4. **Uploads** the metadata blobs to those pre-signed URLs (which point at Onehouse-owned object storage).
5. **Checkpoints** the per-table progress back to the control plane so the next tick is incremental.

Base data files are never read. Only metadata files leave the customer cloud, and only into the pre-signed URL the control plane hands out.

```
+--------------------------------------+      +-----------------------------+
|  customer data plane                 |      |  Onehouse control plane     |
|                                      | (1)  |  POST /v1/community/{id}/   |
|  metadata-extractor pod              |----->|       upload-urls           |
|  (this codebase)                     |  <---|  (presigned PUT URLs)       |
|                                      |      |                             |
|  reads .hoodie/* (Hudi)              |      |                             |
|  reads metadata/* (Iceberg)          |  (2) |                             |
|  from customer-owned S3/GCS/Azure    |--+   |                             |
+--------------------------------------+  |   +-----------------------------+
                                          |       PUT presigned URL
                                          +-----> +--------------------------+
                                                  | Onehouse-owned bucket    |
                                                  | (LakeView metadata blob) |
                                                  +--------------------------+
                                          (3) POST /v1/community/{id}/checkpoint
```

## 2. Process lifecycle

`Main` is the entry point.

| Step | Class · Method | Notes |
|---|---|---|
| 1. Parse CLI | `Main#start` via `CliParser#parse` | Accepts `-p <path>` (config file) or `-c <yaml>` (inline YAML). |
| 2. Load config | `ConfigLoader#loadConfigFromConfigFile` / `loadConfigFromString` | YAML → `ConfigV1`. Auth secrets may be split off into a separate file referenced by `onehouseClientConfig.file`. Validates required fields and positivity of interval/batch settings. |
| 3. Build DI graph | Guice `RuntimeModule` + `MetricsModule` | Creates the shared `ExecutorService`, `OkHttpClient`, `AsyncHttpClientWithRetry`, storage clients, metrics. |
| 4. Optional config refresher | `ConfigRefresher` | Polls the control plane for live config updates if a metadata-extractor config path is supplied externally. |
| 5. Dispatch run mode | `Main#runJob` | `CONTINUOUS` → `TableDiscoveryAndUploadJob#runInContinuousMode` (long-lived). `ONCE` / `ONCE_WITH_RETRY` → `runOnce` then `shutdown`. |
| 6. Shutdown | `Main#shutdown` | Sleeps `waitTimeBeforeShutdown` seconds, then shuts down scheduler, job, metrics server, and config refresher. |

Environment variables and system properties that change behaviour:

| Variable | Purpose | Default |
|---|---|---|
| `ONEHOUSE_API_ENDPOINT` | Control-plane base URL | `https://api.onehouse.ai` |
| `HTTP_PROXY` | OkHttp proxy | unset |
| `NO_PROXY` | OkHttp proxy bypass list | unset |

## 3. Table discovery

`metadata_extractor.TableDiscoveryService` is responsible.

### Inputs

- `metadataExtractorConfig.parserConfig[].databases[].basePaths` — the prefixes to scan.
- `database.tableFormat` — `HUDI` (default) or `ICEBERG`. Picks the matching `TableFormatDetector`.
- `metadataExtractorConfig.pathExclusionPatterns` — optional regex list. Sub-paths matching any pattern are skipped during recursion.
- `database.tableHints[tableId].metadataLocationHint` — optional control-plane-supplied hint, looked up after discovery (only for base paths pinned with an explicit `#tableId` suffix).

### Walk

`discoverTablesInPath(path, lakeName, databaseName, exclusions, detector)`:

```
listAllFilesInDir(path)
  └── detector.matches(path, listedFiles) ──┐
                                            │
                  matches == true ──────────┘──► emit Table { absoluteTableUri, lake, database, format }
                                            │
                  matches == false ─────────┘──► for each subdirectory:
                                                    if !excluded: recurse
                                                  return union of subtree results
```

For Hudi, `HudiTableFormatDetector.matches` returns `true` iff the directory contains a `.hoodie/` folder. For Iceberg, the equivalent is `metadata/` plus a recognisable `metadata.json`.

### Parallelism

The whole walk runs on the shared `ExecutorService` (`RuntimeModule#providesExecutorService`) — a `ForkJoinPool` sized at `Runtime.getRuntime().availableProcessors() * 5` with `asyncMode = true`. Each subdirectory recursion is its own `CompletableFuture`; the parent joins all of them via `CompletableFuture.allOf` before returning its accumulated `Set<Table>`.

### Scheduling

In `CONTINUOUS` mode, `TableDiscoveryAndUploadJob` runs two scheduled tasks on a 2-thread `ScheduledExecutorService`:

| Task | Interval (config) | Default |
|---|---|---|
| Discovery | `tableDiscoveryIntervalMinutes` | 30 min |
| Metadata upload | `tableMetadataUploadIntervalMinutes` | 5 min |

Discovery writes its `Set<Table>` result into a synchronised field `tablesToProcess`; the upload tick reads from that field.

## 4. Metadata read path (Hudi)

`TableMetadataUploaderService#uploadInstantsInTable` orchestrates three reads per table:

### 4.1 `hoodie.properties`

`HoodiePropertiesReader.readHoodieProperties(path)` reads `{base}/.hoodie/hoodie.properties`, parses it into a `ParsedHudiProperties` (`tableName`, `tableType`, `tableVersion`, `timelineLayoutVersion`). Failures (missing file, parse error, missing required keys) are caught into the `exceptionally(...)` block, emit a `HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED` metric, and return a sentinel `ParsedHudiProperties` whose `metadataUploadFailureReasons` field tells the upper layer to skip the table.

### 4.2 Active timeline

`TimelineCommitInstantsUploader#paginatedBatchUploadWithCheckpoint` lists `.hoodie/<instant>.<action>` files page by page using `asyncStorageClient.fetchObjectsByPage` (continuation-token paginated, NOT a full LIST per tick). Filenames are filtered by `ACTIVE_COMMIT_INSTANT_PATTERN = \d+(_\d+)?(\.[a-z]{1,20}){1,2}` to drop unrelated files. Files are then **grouped into batches by `ActiveTimelineInstantBatcher`** so that all files belonging to one Hudi instant land in the same batch, then uploaded in batches of `presignedUrlRequestBatchSizeActiveTimeline` (default **20** instants).

### 4.3 Archived timeline

Two layouts are supported, selected by `table.getTimelineLayoutVersion()`:

| Layout | Path | Discovery | Reader |
|---|---|---|---|
| **V1** (flat) | `.hoodie/archived/.commits_.archive.N-M-K` | Full directory LIST every tick | `TimelineCommitInstantsUploader#executeFullBatchUpload` |
| **V2** (LSM tree) | `.hoodie/timeline/history/` with `_version_` + `manifest_N` | Read `_version_` then matching `manifest_N` only | `TimelineCommitInstantsUploader#executeManifestDrivenArchivedUpload` via `LSMTimelineManifestReader` |

V2 is meaningfully more efficient — only the files referenced by the current manifest version are uploaded. V1 re-scans the full archived directory every tick.

Archived batches use the smaller `presignedUrlRequestBatchSizeArchivedTimeline` (default **2** instants per API call).

## 5. Pre-signed URL fetch

`api.OnehouseApiClient` is the only class that talks to the control plane.

### Endpoint

```
POST {ONEHOUSE_API_ENDPOINT}/v1/community/{tableId}/upload-urls
Content-Type: application/json
x-onehouse-project-uid: <projectId>
x-onehouse-api-key: <apiKey>
x-onehouse-api-secret: <apiSecret>
x-onehouse-uuid: <userId>
x-onehouse-region: <region?>
x-onehouse-link-uid: <requestId?>
x-onehouse-trace-request-uuid: <generated-per-request>

{
  "tableId": "...",
  "commitInstants": ["20260101120000.commit", ...],
  "commitTimelineType": "COMMIT_TIMELINE_TYPE_ACTIVE" | "COMMIT_TIMELINE_TYPE_ARCHIVED"
}
```

The response is a list of pre-signed PUT URLs, one per instant filename, ordered the same as the request.

### Auth & headers

Headers are constructed once in `OnehouseApiClient`'s constructor from `Config.getOnehouseClientConfig` and reused for every call (header map is immutable after init). The `x-onehouse-trace-request-uuid` is generated per call and threaded through MDC so retried log lines all share the trace id.

### Related endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/v1/community/initialize-tables` | POST | First-time table registration; control plane returns an initial `Checkpoint`. |
| `/v1/community/checkpoints?tableIds=...` | GET | Batch fetch checkpoints for known tables at the start of an upload tick. |
| `/v1/community/{tableId}/checkpoint` | POST | Advance per-table checkpoint after a successful batch. |
| `/v1/community/{tableId}/upload-urls` | POST | Pre-signed URL batch for one timeline-type. |

## 6. Upload path

`storage.PresignedUrlFileUploader#uploadFileToPresignedUrl(presignedUrl, fileUrl, fileUploadStreamBatchSize)`:

```
asyncStorageClient.streamFileAsync(fileUrl)
  └── thenCompose(fileStreamData -> runAsync {
        Request request = getRequest(presignedUrl, batchSize, fileStreamData);
        asyncHttpClientWithRetry.makeRequestWithRetry(request)
            .thenAccept(this::checkResponseOrThrow)
            .join();
      })
```

`getRequest` has two branches based on `fileSize` vs `fileUploadStreamBatchSize` (default 5 MiB):

| Branch | Strategy | Notes |
|---|---|---|
| **Small file** (`fileSize <= batchSize`) | Buffer the whole file into a `byte[]` with `IOUtils.toByteArray`, send as a single PUT with `application/octet-stream`. | InputStream is closed via try-with-resources. |
| **Large file** | Use OkHttp streaming `RequestBody` whose `writeTo(BufferedSink)` reads the stream in `batchSize` chunks. | InputStream is closed via try-with-resources inside `writeTo`. |

No compression, no multipart. One PUT per file. Files within a batch upload in parallel via `CompletableFuture.allOf`. After all files in the batch succeed, `TimelineCommitInstantsUploader.uploadBatch` advances the checkpoint via `OnehouseApiClient.upsertTableMetricsCheckpoint`.

## 7. HTTP transport: retries, timeouts, jitter

The HTTP wrapper is `api.AsyncHttpClientWithRetry`. Every external call — control-plane API and pre-signed URL upload alike — goes through it.

| Setting | Source | Default |
|---|---|---|
| Read / write / connect timeout | OkHttp builder in `RuntimeModule` | 15 s each |
| Max concurrent requests | `MetadataExtractorConfig.nettyMaxConcurrency` | 50 (OkHttp dispatcher default is 64) |
| Max retries | `RuntimeModule` | 3 |
| Base retry delay | `RuntimeModule` | 1 000 ms |
| Max retry delay | `MAX_RETRY_DELAY_MILLIS` | 10 000 ms |

### Retry decision table

```
                    ┌───────────────────────────────────┐
                    │   response from OkHttp callback   │
                    └─────────────────┬─────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │                             │                             │
        ▼                             ▼                             ▼
 IOException                  response.code() in              everything else
 (timeout, reset, etc.)       ACCEPTABLE_HTTP_FAILURE         (success 2xx or 5xx)
        │                     {400,401,403,404,409}                 │
        ▼                             │                             ▼
 retry up to maxRetries               ▼                       if !isSuccessful
                              future.complete(response)       && tryCount < maxRetries
                              (no retry, caller decides)         retry
                                                              else
                                                                 future.complete(response)
```

### Backoff formula

```
delay  = retryDelayMillis * 2^tryCount       // tryCount starts at 1
jitter = (random.nextDouble() * delay) - delay/2
next   = min(delay + jitter, MAX_RETRY_DELAY_MILLIS)
```

So with the default `retryDelayMillis = 1000` and `maxRetries = 3`, the actual wait between attempts is roughly `2 000 ± 1 000` ms then `4 000 ± 2 000` ms, capped at 10 s.

## 8. State and checkpointing

The unit of state is a per-table `Checkpoint`:

| Field | Meaning |
|---|---|
| `batchId` | Monotonic counter incremented per successful batch. |
| `checkpointTimestamp` | Last-modified time of the most-recent uploaded file. |
| `lastUploadedFile` | Filename of the most-recent uploaded instant. |
| `firstIncompleteCommitFile` | When `ContinueOnIncompleteCommitStrategy` is active, remembers the first incomplete commit so subsequent ticks can re-attempt it. |
| `archivedCommitsProcessed` | Set to `true` after the first active-timeline batch completes; gates the order of archived vs active upload. |
| `lastArchivedManifestVersion` | V2 archived-timeline cursor — manifest version processed in the last tick. |

`TableMetadataUploaderService#uploadInstantsInTable` fetches the checkpoint for every active table via `OnehouseApiClient.getTableMetricsCheckpoints` at the start of each upload tick. New tables (no checkpoint) are first registered via `initializeTableMetricsCheckpoint` which seeds an initial cursor.

Incrementality is filename-based (`> lastUploadedFile` lexicographically for active, numeric for archived) AND timestamp-based (`>= checkpointTimestamp`, when `applyLastModifiedAtFilter == true`). **There is no content-hash check** — if the same instant file is rewritten with a newer mtime, it is re-uploaded.

## 9. Error handling matrix

| Failure | Behaviour | Where |
|---|---|---|
| Network `IOException` | Retried up to 3× with exponential backoff | `AsyncHttpClientWithRetry#onFailure` |
| HTTP 5xx | Retried up to 3× with exponential backoff | `AsyncHttpClientWithRetry#onResponse` |
| HTTP 400/401/403/404/409 | No retry, surfaced as a `RuntimeException` carrying status + message | `OnehouseApiClient#handleResponse` |
| Upload PUT non-2xx | `FileUploadException`; batch aborts; `PRESIGNED_URL_UPLOAD_FAILURE` metric | `PresignedUrlFileUploader#uploadFileToPresignedUrl` |
| `hoodie.properties` missing/corrupt | Table skipped; `HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED` metric | `HoodiePropertiesReader#readHoodieProperties` |
| Storage list/read failure | Table skipped; table-level failure metric | various, via `.exceptionally(...)` |
| Discovery failure on a path | Path skipped; log warning; continue at next tick | `TableDiscoveryService#discoverTablesInPath` |
| Checkpoint write failure | Batch aborts; next tick re-tries from previous checkpoint | `TimelineCommitInstantsUploader#uploadBatch` |
| Empty/malformed YAML config | `RuntimeException` wrapping `IllegalArgumentException` with a precise message | `ConfigLoader#loadConfigFromJsonNode` |

## 10. Concurrency model

| Pool | Size | Owner | Purpose |
|---|---|---|---|
| `ExecutorService` (ForkJoinPool) | `cores * 5`, asyncMode | `RuntimeModule` | All filesystem walks, OkHttp dispatch, `CompletableFuture` async stages |
| `ScheduledExecutorService` (retry) | 1 | `AsyncHttpClientWithRetry` | Scheduling retry attempts |
| `ScheduledExecutorService` (tick) | 2 | `TableDiscoveryAndUploadJob` | Discovery tick + upload tick |

Files within an upload batch run in parallel via `CompletableFuture.allOf`. Batches within a table and tables within a tick are sequential.

## 11. Cost levers (current state — not necessarily bugs)

- **No content de-duplication.** Re-writing an instant file with the same content but a newer mtime triggers a full re-upload.
- **V1 archived timeline is full-scan every tick.** V2 manifest path is already incremental — customers on V1 pay more.
- **Table discovery re-walks every base path every tick.** No memoisation of "this prefix produced no new tables since timestamp T."
- **Archived-timeline API batch size of 2** is tiny — each `upload-urls` call carries only 2 instants. Worth measuring vs raising to 10+.
- **No upload compression.** Each instant file is small (< 1 KB typical), but archived V1 timelines with thousands of files still add up.

## 12. Configuration reference

`ConfigLoader` accepts YAML conforming to `ConfigV1`. Top-level keys:

| Key | Required | Notes |
|---|---|---|
| `version` | yes | Must be `V1`. As of [ENG-43320 hardening PR], missing/blank/invalid values surface a clear `IllegalArgumentException` at load time instead of a buried `NullPointerException`. |
| `onehouseClientConfig` | yes | Either inline `{projectId, apiKey, apiSecret, userId, region?, requestId?}` or `{file: <path>}` pointing to a separate YAML/JSON. Mixing the two is allowed — inline values win. |
| `fileSystemConfiguration` | yes | One of `s3Config`, `gcsConfig`, `azureBlobConfig`, or `fileSystemHadoopConfig` (for non-cloud filesystems). |
| `metadataExtractorConfig.parserConfig[]` | yes | List of `{lake?, databases: [{name, basePaths, tableFormat?, tableHints?}]}`. |
| `metadataExtractorConfig.tableDiscoveryIntervalMinutes` | no | Default 30. Must be ≥ 1. |
| `metadataExtractorConfig.tableMetadataUploadIntervalMinutes` | no | Default 5. Must be ≥ 1. |
| `metadataExtractorConfig.processTableMetadataSyncDurationSeconds` | no | Must be ≥ 1. |
| `metadataExtractorConfig.presignedUrlRequestBatchSizeActiveTimeline` | no | Default 20. Must be ≥ 1. |
| `metadataExtractorConfig.presignedUrlRequestBatchSizeArchivedTimeline` | no | Default 2. Must be ≥ 1. |
| `metadataExtractorConfig.pathExclusionPatterns` | no | Regex list applied during the discovery walk. |

## 13. Test coverage map (HEAD `3275c3f`)

| Production class | Test class | State |
|---|---|---|
| `OnehouseApiClient` | `OnehouseApiClientTest` | 4 tests / 369 LOC — happy paths covered, error branches thin |
| `AsyncHttpClientWithRetry` | `AsyncHttpClientWithRetryTest` | 3 tests / 86 LOC — no jitter/backoff bounds, no acceptable-4xx no-retry, no shutdown |
| `PresignedUrlFileUploader` | `PresignedUrlFileUploaderTest` | 6 tests — error paths added in [ENG-43320 PR #192] |
| `HoodiePropertiesReader` | `HoodiePropertiesReaderTest` | 5 tests including missing-required-keys |
| `ConfigLoader` | `ConfigLoaderTest` | Happy paths + version-field validation added in this PR |
| `TableDiscoveryService` | `TableDiscoveryServiceTest` | ✓ |
| `TableMetadataUploaderService` | `TableMetadataUploaderServiceTest` | ✓ |
| `TimelineCommitInstantsUploader` | `TimelineCommitInstantsUploaderTest` | ✓ (largest test class) |
| `IcebergMetadataUploaderService` | `TestIcebergMetadataUploaderService` | ✓ |
| `*AsyncStorageClient` (S3 / GCS / Azure) | `*AsyncStorageClientTest` | ✓ |
| `StorageUtils` | `StorageUtilsTest` | ✓ |
| `LSMTimelineManifestReader` | `LSMTimelineManifestReaderTest` | ✓ |
| `ActiveTimelineInstantBatcher` | `ActiveTimelineInstantBatcherTest` | ✓ |
| `HudiTableFormatDetector` / `IcebergTableFormatDetector` | `Test*TableFormatDetector` | ✓ |
| `ContinueOnIncompleteCommitStrategy` | `ContinueOnIncompleteCommitStrategyTest` | ✓ |
| `ConfigRefresher` | `ConfigRefresherTest` | ✓ |
| `MetadataExtractorUtils` | `MetadataExtractorUtilsTest` | ✓ |

The remaining test-coverage gaps worth filling (ranked):

1. `AsyncHttpClientWithRetry` — backoff math, jitter bounds, acceptable-4xx no-retry assertion, scheduler shutdown.
2. `OnehouseApiClient` — `handleResponse` 401-message override, 5xx error, deserialization-failure branches.
3. `TableDiscoveryService` — duplicate `tableHint` conflict, path-exclusion regex edge cases, nested-table detection.

## 14. Debugging recipes

### Where did a particular instant file get uploaded?

1. Filter pod logs by `tableId` (every upload log line carries it).
2. Look for `Uploading batch` log entries in `TimelineCommitInstantsUploader#uploadBatch`.
3. Cross-reference with the `x-onehouse-trace-request-uuid` from the `upload-urls` API call to follow the round-trip through both pod logs and control-plane logs.

### Why did the extractor stop progressing on a table?

1. Check the table's checkpoint via the control plane (`GET /v1/community/checkpoints?tableIds=<tid>`).
2. Look for `FAILURE` metrics with reason `HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED`, `PRESIGNED_URL_UPLOAD_FAILURE`, or `API_FAILURE_USER_ERROR` for that table.
3. If the table is "stuck" but no failures are visible, suspect an incomplete commit — check whether `ContinueOnIncompleteCommitStrategy` has a `firstIncompleteCommitFile` set in the checkpoint.

### Why are we re-uploading the same files?

1. The extractor uses filename + mtime for incrementality. Any process that rewrites Hudi instant files in place will trigger re-uploads.
2. For Hudi V1 archived timelines, every tick re-LISTS `.hoodie/archived/`. V2 LSM tables only re-read the current `manifest_N`.

### Config didn't parse — where do I look?

1. As of [ENG-43320 hardening PR], `ConfigLoader` emits precise errors: `Config missing required 'version' field`, `Unsupported config version: <X>`, `Config is empty or could not be parsed`, plus `Failed to load config from <path>` when reading the file fails.
2. If you see a `NullPointerException` instead, the failure is downstream of YAML parsing — likely missing required nested fields. Look at `validateOnehouseClientConfig` for the canonical list.

## 15. Related docs

- [`overview.md`](overview.md) — short-form intro to the repo.
- [`getting-started.md`](getting-started.md) — local development setup.
- [`iceberg-support.md`](iceberg-support.md) — Iceberg-specific discovery and metadata.json selection.
