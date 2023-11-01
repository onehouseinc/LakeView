package com.onehouse.metadataExtractor;

import static com.onehouse.metadataExtractor.Constants.HOODIE_FOLDER_NAME;
import static com.onehouse.metadataExtractor.Constants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.metadataExtractor.Constants.INITIAL_CHECKPOINT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.metadataExtractor.models.Checkpoint;
import com.onehouse.metadataExtractor.models.Table;
import com.onehouse.storage.AsyncStorageLister;
import com.onehouse.storage.S3AsyncFileUploader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

public class TableMetadataUploaderService {
  private final AsyncStorageLister asyncStorageLister;
  private final HoodiePropertiesReader hoodiePropertiesReader;
  private final S3AsyncFileUploader s3AsyncFileUploader;
  private final OnehouseApiClient onehouseApiClient;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;

  @Inject
  public TableMetadataUploaderService(
      @Nonnull AsyncStorageLister asyncStorageLister,
      @Nonnull HoodiePropertiesReader hoodiePropertiesReader,
      @Nonnull S3AsyncFileUploader s3AsyncFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageLister = asyncStorageLister;
    this.hoodiePropertiesReader = hoodiePropertiesReader;
    this.s3AsyncFileUploader = s3AsyncFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.executorService = executorService;
    this.mapper = new ObjectMapper();
  }

  public CompletableFuture<Void> processTables(Set<Table> tablesToProcess) {
    List<CompletableFuture<Void>> processTablesFuture = new ArrayList<>();
    for (Table table : tablesToProcess) {
      processTablesFuture.add(processTable(table));
    }

    return CompletableFuture.allOf(processTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(ignored -> null);
  }

  private CompletableFuture<Void> processTable(Table table) {
    UUID tableId = getTableIdFromAbsolutePathUrl(table.getAbsoluteTableUrl());
    return onehouseApiClient
        .getTableMetricsCheckpoint(tableId.toString())
        .thenCompose(
            getTableMetricsCheckpointResponse -> {
              if (getTableMetricsCheckpointResponse.isFailure()
                  && getTableMetricsCheckpointResponse.getStatusCode() == 5) {
                // checkpoint not found, table needs to be registered
                return hoodiePropertiesReader
                    .readHoodieProperties(getHoodiePropertiesFilePath(table))
                    .thenCompose(
                        properties ->
                            onehouseApiClient.initializeTableMetricsCheckpoint(
                                InitializeTableMetricsCheckpointRequest.builder()
                                    .tableId(tableId)
                                    .tableName(properties.getTableName())
                                    .tableType(properties.getTableType())
                                    .tableBasePath(table.getRelativeTablePath())
                                    .databaseName(table.getDatabaseName())
                                    .lakeName(table.getLakeName())
                                    .build()))
                    .thenCompose(
                        initializeTableMetricsCheckpointResponse -> {
                          if (!initializeTableMetricsCheckpointResponse.isFailure()) {
                            return processInstantsInTable(INITIAL_CHECKPOINT);
                          }
                          throw new RuntimeException(
                              "Failed to initialise table for processing, " + table);
                        });
              }
              try {
                // process from previous checkpoint
                return processInstantsInTable(
                    mapper.readValue(
                        getTableMetricsCheckpointResponse.getCheckpoint(), Checkpoint.class));
              } catch (JsonProcessingException e) {
                throw new RuntimeException("Error deserializing checkpoint value", e);
              }
            });
  }

  private CompletableFuture<Void> processInstantsInTable(Checkpoint checkpoint) {
    // TODO: fill in code
    return CompletableFuture.completedFuture(null);
  }

  private String getHoodiePropertiesFilePath(Table table) {
    String basePath = table.getAbsoluteTableUrl();
    return String.format(
        "%s/%s/%s",
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath,
        HOODIE_FOLDER_NAME,
        HOODIE_PROPERTIES_FILE);
  }

  private static UUID getTableIdFromAbsolutePathUrl(String tableAbsolutePathUrl) {
    return UUID.nameUUIDFromBytes(tableAbsolutePathUrl.getBytes());
  }
}
