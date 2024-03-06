package com.onehouse.constants;

import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class MetadataExtractorConstants {
  private MetadataExtractorConstants() {}

  public static final String HOODIE_FOLDER_NAME = ".hoodie";
  public static final String ARCHIVED_FOLDER_NAME = "archived";
  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
  public static final String HOODIE_TABLE_TYPE_KEY = "hoodie.table.type";

  // The default number of instants in one archived commit metadata file is 10
  // so we want to ingest 10x active instants than archived instants in one batch
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE = 20;
  public static final int PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE = 2;
  public static final int TABLE_PROCESSING_BATCH_SIZE =
      Math.min(
          50,
          Integer.parseInt(
              System.getenv().getOrDefault("EXTRACTOR_TABLE_PROCESSING_BATCH_SIZE", "20")));
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

  public static final String SAVEPOINT_ACTION = "savepoint";
  public static final String ROLLBACK_ACTION = "rollback";
  public static final List<String> WHITELISTED_ACTION_TYPES =
      Arrays.asList(
          "commit",
          "deltacommit",
          ROLLBACK_ACTION,
          SAVEPOINT_ACTION,
          "restore",
          "clean",
          "compaction",
          "replacecommit");
}
