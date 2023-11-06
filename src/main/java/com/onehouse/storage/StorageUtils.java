package com.onehouse.storage;

import static com.onehouse.constants.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  private static final String INVALID_STORAGE_URI_ERROR_MSG = "Invalid Object storage Uri: ";

  public String getPathFromUrl(String uri) {

    if (!OBJECT_STORAGE_URI_PATTERN.matcher(uri).matches()) {
      throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
    }

    String prefix = "";

    // Remove the scheme and bucket name from the S3 path
    int startIndex = uri.indexOf('/', 5); // Skip 's3://' and 'gs://'
    if (startIndex != -1) {
      prefix = uri.substring(startIndex + 1);
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

  public String getBucketNameFromUri(String uri) {
    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
  }
}
