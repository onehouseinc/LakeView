package com.onehouse.metadataExtractor;

import com.onehouse.metadataExtractor.models.Checkpoint;
import java.time.Instant;

public class Constants {
  public static final String HOODIE_FOLDER_NAME = ".hoodie";
  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE = 100;
  public static final int TABLE_DISCOVERY_INTERVAL_MINUTES = 30;
  public static final int TABLE_METADATA_UPLOAD_INTERVAL_MINUTES = 5;
  public static final Checkpoint INITIAL_CHECKPOINT =
      Checkpoint.builder()
          .batchId(-1)
          .checkpoint(Instant.EPOCH)
          .lastUploadedFile("")
          .isArchivedCommitsProcessed(false)
          .build();
}
