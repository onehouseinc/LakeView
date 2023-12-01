package com.onehouse.constants;

import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.util.regex.Pattern;

public class MetadataExtractorConstants {
  private MetadataExtractorConstants() {}

  public static final String HOODIE_FOLDER_NAME = ".hoodie";
  public static final String ARCHIVED_FOLDER_NAME = "archived";
  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
  public static final String HOODIE_TABLE_TYPE_KEY = "hoodie.table.type";
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE = 100;
  public static final int TABLE_PROCESSING_BATCH_SIZE = 10;
  public static final int TABLE_DISCOVERY_INTERVAL_MINUTES = 30;
  public static final int TABLE_METADATA_UPLOAD_INTERVAL_MINUTES = 5;
  public static final Pattern ARCHIVED_COMMIT_INSTANT_PATTERN =
      Pattern.compile("\\.commits_\\.archive\\.\\d+_\\d+-\\d+-\\d+");
  public static final Pattern ACTIVE_COMMIT_INSTANT_PATTERN =
      Pattern.compile("\\d+(\\.[a-z]{1,20}){1,2}");
  public static final Checkpoint INITIAL_CHECKPOINT =
      Checkpoint.builder()
          .batchId(0)
          .checkpointTimestamp(Instant.EPOCH)
          .lastUploadedFile("")
          .archivedCommitsProcessed(false)
          .build();

  // hardcoding last modified at to prevent this from causing issues with our checkpoint logic
  public static final File HOODIE_PROPERTIES_FILE_OBJ =
      File.builder()
          .filename(HOODIE_PROPERTIES_FILE)
          .isDirectory(false)
          .lastModifiedAt(Instant.EPOCH)
          .build();
}
