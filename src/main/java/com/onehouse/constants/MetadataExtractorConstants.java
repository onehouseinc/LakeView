package com.onehouse.constants;

import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.storage.models.File;
import java.time.Instant;

public class MetadataExtractorConstants {
  private MetadataExtractorConstants() {}

  public static final String HOODIE_FOLDER_NAME = ".hoodie";
  public static final String ARCHIVED_FOLDER_NAME = "archived";
  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
  public static final String HOODIE_TABLE_TYPE_KEY = "hoodie.table.type";
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE = 100;
  public static final int TABLE_DISCOVERY_INTERVAL_MINUTES = 30;
  public static final int TABLE_METADATA_UPLOAD_INTERVAL_MINUTES = 5;
  public static final Checkpoint INITIAL_ARCHIVED_TIMELINE_CHECKPOINT =
      Checkpoint.builder()
          .batchId(0)
          .checkpointTimestamp(Instant.EPOCH)
          .lastUploadedFile("")
          .archivedCommitsProcessed(false)
          .build();
  public static final Checkpoint INITIAL_ACTIVE_TIMELINE_CHECKPOINT =
      Checkpoint.builder()
          .batchId(0)
          .checkpointTimestamp(Instant.EPOCH)
          .lastUploadedFile("")
          .archivedCommitsProcessed(true)
          .build();

  // hardcoding last modified at to prevent this from causing issues with our checkpoint logic
  public static final File HOODIE_PROPERTIES_FILE_OBJ =
      File.builder()
          .filename(HOODIE_PROPERTIES_FILE)
          .isDirectory(false)
          .lastModifiedAt(Instant.EPOCH)
          .build();
}
