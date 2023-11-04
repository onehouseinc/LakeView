package com.onehouse.metadata_extractor;

import com.onehouse.metadata_extractor.models.Checkpoint;
import java.time.Instant;

public class Constants {
  private Constants() {}

  public static final String HOODIE_FOLDER_NAME = ".hoodie";
  public static final String ARCHIVED_FOLDER_NAME = "archived";
  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
  public static final String HOODIE_TABLE_TYPE_KEY = "hoodie.table.type";
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE = 100;
  public static final int TABLE_DISCOVERY_INTERVAL_MINUTES = 30;
  public static final int TABLE_METADATA_UPLOAD_INTERVAL_MINUTES = 5;
  public static final Checkpoint INITIAL_CHECKPOINT =
      Checkpoint.builder()
          .batchId(0)
          .checkpointTimestamp(Instant.EPOCH)
          .lastUploadedFile("")
          .archivedCommitsProcessed(false)
          .build();
}
