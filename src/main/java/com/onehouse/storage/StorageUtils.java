package com.onehouse.storage;

import static com.onehouse.storage.StorageConstants.GCS_PATH_PATTERN;
import static com.onehouse.storage.StorageConstants.S3_PATH_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  public String getPathFromUrl(String url) {
    String prefix = "";

    // Remove the scheme and bucket name from the S3 path
    int startIndex = url.indexOf('/', 5); // Skip 's3://'
    if (startIndex != -1) {
      prefix = url.substring(startIndex + 1);
    }

    return prefix;
  }

  public String constructFilePath(String directoryPath, String filePath) {
    return String.format(
        "%s/%s",
        directoryPath.endsWith("/")
            ? directoryPath.substring(0, directoryPath.length() - 1)
            : directoryPath,
        filePath);
  }

  public String getS3BucketNameFromS3Url(String s3Path) {
    Matcher matcher = S3_PATH_PATTERN.matcher(s3Path);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid AWS S3 path: " + s3Path);
  }

  public String getGcsBucketNameFromPath(String gcsUrl) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(gcsUrl);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid GCS path: " + gcsUrl);
  }
}
