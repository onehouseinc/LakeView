package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;
import static ai.onehouse.constants.MetadataExtractorConstants.ICEBERG_METADATA_FILE_SUFFIX;
import static ai.onehouse.constants.MetadataExtractorConstants.ICEBERG_METADATA_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.INITIAL_CHECKPOINT;
import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;

import ai.onehouse.api.OnehouseApiClient;
import ai.onehouse.api.models.request.CommitTimelineType;
import ai.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import ai.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import ai.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest.InitializeSingleTableMetricsCheckpointRequest;
import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.api.models.request.TableType;
import ai.onehouse.api.models.request.UploadedFile;
import ai.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import ai.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import ai.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.PresignedUrlFileUploader;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import ai.onehouse.RuntimeModule.TableDiscoveryObjectStorageAsyncClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.inject.Inject;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Discovers and uploads Iceberg table metadata pointer files ({@code metadata/*.metadata.json}).
 *
 * <p>Iceberg's snapshot-summary contract means a single {@code metadata.json} contains all of:
 * cumulative {@code total-records}, {@code total-files-size}, {@code total-data-files},
 * per-snapshot deltas, and {@code snapshot-log[]} history. We therefore upload exactly one file
 * per iteration — the latest {@code metadata.json}. The control plane parses snapshot summaries
 * from it and writes metrics to OpenSearch.
 *
 * <p>Parallel to {@link TableMetadataUploaderService} (the Hudi orchestrator) rather than a
 * subclass: Hudi's active/archived timeline distinction, hoodie.properties bootstrap, and LSM
 * manifest plumbing don't apply, so a separate service is clearer than a branching megaclass.
 * Shared primitives ({@link OnehouseApiClient}, {@link PresignedUrlFileUploader}, {@link
 * AsyncStorageClient}) are reused.
 */
@Slf4j
public class IcebergMetadataUploaderService {
  private final AsyncStorageClient asyncStorageClient;
  private final OnehouseApiClient onehouseApiClient;
  private final PresignedUrlFileUploader presignedUrlFileUploader;
  private final StorageUtils storageUtils;
  private final LakeViewExtractorMetrics metrics;
  private final ObjectMapper mapper;

  @Inject
  public IcebergMetadataUploaderService(
      @Nonnull @TableDiscoveryObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull StorageUtils storageUtils,
      @Nonnull LakeViewExtractorMetrics metrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.onehouseApiClient = onehouseApiClient;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.storageUtils = storageUtils;
    this.metrics = metrics;
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
  }

  /**
   * Uploads the latest metadata.json for each Iceberg table whose pointer file has advanced since
   * the last checkpoint. Returns true only if every table either succeeded or was already
   * up-to-date.
   */
  public CompletableFuture<Boolean> uploadInstantsInTables(Set<Table> tables) {
    if (tables.isEmpty()) {
      return CompletableFuture.completedFuture(true);
    }
    log.info("Uploading Iceberg metadata for {} table(s)", tables.size());
    List<CompletableFuture<Boolean>> perTable =
        tables.stream().map(this::processTable).collect(Collectors.toList());
    return CompletableFuture.allOf(perTable.toArray(new CompletableFuture[0]))
        .thenApply(
            ignored -> perTable.stream().map(CompletableFuture::join).allMatch(Boolean.TRUE::equals));
  }

  private CompletableFuture<Boolean> processTable(Table table) {
    return onehouseApiClient
        .getTableMetricsCheckpoints(Collections.singletonList(table.getTableId()))
        .thenComposeAsync(
            response -> {
              if (response.isFailure()) {
                log.error(
                    "Failed to fetch checkpoint for Iceberg table {} (status {}): {}",
                    table.getTableId(),
                    response.getStatusCode(),
                    response.getCause());
                return CompletableFuture.completedFuture(false);
              }
              Optional<Checkpoint> existing = parseCheckpoint(response, table.getTableId());
              if (existing.isPresent()) {
                return uploadIfNewMetadataJson(table, existing.get());
              }
              return initialiseTable(table)
                  .thenComposeAsync(
                      initOk -> {
                        if (!initOk) {
                          return CompletableFuture.completedFuture(false);
                        }
                        return uploadIfNewMetadataJson(table, INITIAL_CHECKPOINT);
                      });
            })
        .exceptionally(
            e -> {
              log.error("Exception processing Iceberg table {}", table.getTableId(), e);
              metrics.incrementTableMetadataProcessingFailureCounter(
                  getMetadataExtractorFailureReason(
                      e, MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                  String.format("Iceberg upload exception: %s", e.getMessage()));
              return false;
            });
  }

  private Optional<Checkpoint> parseCheckpoint(
      GetTableMetricsCheckpointResponse response, String tableId) {
    return response.getCheckpoints().stream()
        .filter(c -> tableId.equals(c.getTableId()))
        .findFirst()
        .map(GetTableMetricsCheckpointResponse.TableMetadataCheckpoint::getCheckpoint)
        .filter(StringUtils::isNotBlank)
        .map(
            json -> {
              try {
                return mapper.readValue(json, Checkpoint.class);
              } catch (JsonProcessingException e) {
                log.error("Malformed checkpoint JSON for Iceberg table {}; treating as missing", tableId, e);
                return null;
              }
            });
  }

  private CompletableFuture<Boolean> initialiseTable(Table table) {
    String tableName = deriveTableName(table.getAbsoluteTableUri());
    InitializeSingleTableMetricsCheckpointRequest single =
        InitializeSingleTableMetricsCheckpointRequest.builder()
            .tableId(table.getTableId())
            .tableName(tableName)
            // COW is a meaningless placeholder for Iceberg; server discriminates on tableFormat.
            .tableType(TableType.COPY_ON_WRITE)
            .tableFormat(TableFormat.ICEBERG)
            .lakeName(table.getLakeName())
            .databaseName(table.getDatabaseName())
            .tableBasePath(table.getAbsoluteTableUri())
            .build();
    return onehouseApiClient
        .initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tables(Collections.singletonList(single))
                .build())
        .thenApply(
            initResponse -> {
              if (initResponse.isFailure()) {
                log.error(
                    "Failed to initialise Iceberg table {} (status {}): {}",
                    table.getTableId(),
                    initResponse.getStatusCode(),
                    initResponse.getCause());
                return false;
              }
              return true;
            });
  }

  private CompletableFuture<Boolean> uploadIfNewMetadataJson(Table table, Checkpoint checkpoint) {
    String metadataDirUri =
        storageUtils.constructFileUri(table.getAbsoluteTableUri(), ICEBERG_METADATA_FOLDER_NAME);
    return asyncStorageClient
        .listAllFilesInDir(metadataDirUri)
        .thenComposeAsync(
            files -> {
              Optional<File> latest = pickLatestMetadataJson(files);
              if (!latest.isPresent()) {
                log.warn(
                    "Iceberg table {} has no metadata.json under {}",
                    table.getTableId(),
                    metadataDirUri);
                return CompletableFuture.completedFuture(true);
              }
              if (latest.get().getFilename().equals(checkpoint.getLastUploadedFile())) {
                log.debug(
                    "Iceberg table {} already up-to-date at {}",
                    table.getTableId(),
                    latest.get().getFilename());
                return CompletableFuture.completedFuture(true);
              }
              return uploadAndAdvanceCheckpoint(table, metadataDirUri, latest.get(), checkpoint);
            });
  }

  private CompletableFuture<Boolean> uploadAndAdvanceCheckpoint(
      Table table, String metadataDirUri, File metadataJson, Checkpoint priorCheckpoint) {
    String fileUri = storageUtils.constructFileUri(metadataDirUri, metadataJson.getFilename());
    return onehouseApiClient
        .generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(table.getTableId())
                .commitInstants(Collections.singletonList(metadataJson.getFilename()))
                // ACTIVE is a placeholder; Iceberg has no archived/active distinction.
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .build())
        .thenComposeAsync(
            urlResponse -> {
              if (urlResponse.isFailure() || urlResponse.getUploadUrls().isEmpty()) {
                log.error(
                    "Failed to obtain presigned URL for {} (status {}): {}",
                    metadataJson.getFilename(),
                    urlResponse.getStatusCode(),
                    urlResponse.getCause());
                return CompletableFuture.completedFuture(false);
              }
              String presigned = urlResponse.getUploadUrls().get(0);
              return presignedUrlFileUploader
                  .uploadFileToPresignedUrl(presigned, fileUri, DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
                  .thenComposeAsync(
                      ignored -> {
                        metrics.incrementMetadataUploadSuccessCounter();
                        return advanceCheckpoint(table, metadataJson, priorCheckpoint);
                      });
            });
  }

  private CompletableFuture<Boolean> advanceCheckpoint(
      Table table, File metadataJson, Checkpoint priorCheckpoint) {
    Checkpoint next =
        priorCheckpoint
            .toBuilder()
            .batchId(priorCheckpoint.getBatchId() + 1)
            .checkpointTimestamp(Instant.now())
            .lastUploadedFile(metadataJson.getFilename())
            // Iceberg has no archived/active distinction; mark archived processed so consumers
            // that interpret the legacy field for ordering don't loop.
            .archivedCommitsProcessed(true)
            .build();
    String checkpointJson;
    try {
      checkpointJson = mapper.writeValueAsString(next);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialise Iceberg checkpoint for table {}", table.getTableId(), e);
      return CompletableFuture.completedFuture(false);
    }
    UpsertTableMetricsCheckpointRequest request =
        UpsertTableMetricsCheckpointRequest.builder()
            .tableId(table.getTableId())
            .checkpoint(checkpointJson)
            .filesUploaded(Collections.singletonList(metadataJson.getFilename()))
            .uploadedFiles(
                Collections.singletonList(
                    UploadedFile.builder()
                        .name(metadataJson.getFilename())
                        .lastModifiedAt(metadataJson.getLastModifiedAt().toEpochMilli())
                        .build()))
            .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
            .build();
    return onehouseApiClient
        .upsertTableMetricsCheckpoint(request)
        .thenApply(
            upsert -> {
              if (upsert.isFailure()) {
                log.error(
                    "Failed to upsert checkpoint for Iceberg table {} (status {}): {}",
                    table.getTableId(),
                    upsert.getStatusCode(),
                    upsert.getCause());
                return false;
              }
              return true;
            });
  }

  /**
   * Picks the latest metadata.json from a {@code metadata/} listing. Both naming conventions in
   * use today — {@code v{N}.metadata.json} (Hadoop catalog) and {@code 00000-<uuid>.metadata.json}
   * (Hive / Glue / Spark) — sort to the same answer lexicographically.
   */
  static Optional<File> pickLatestMetadataJson(List<File> files) {
    return files.stream()
        .filter(f -> !f.isDirectory())
        .filter(f -> f.getFilename().endsWith(ICEBERG_METADATA_FILE_SUFFIX))
        .max((a, b) -> a.getFilename().compareTo(b.getFilename()));
  }

  private static String deriveTableName(String basePathUri) {
    String trimmed = basePathUri.endsWith("/") ? basePathUri.substring(0, basePathUri.length() - 1) : basePathUri;
    int idx = trimmed.lastIndexOf('/');
    return idx < 0 ? trimmed : trimmed.substring(idx + 1);
  }
}
