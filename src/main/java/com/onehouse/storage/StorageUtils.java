package com.onehouse.storage;

import static com.onehouse.storage.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  public String getPathFromUrl(String url) {
    String prefix = "";

    // Remove the scheme and bucket name from the S3 path
    int startIndex = url.indexOf('/', 5); // Skip 's3://' and 'gs://'
    if (startIndex != -1) {
      prefix = url.substring(startIndex + 1);
    }

    return prefix;
  }

  public String getBucketNameFromUri(String uri) {
    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    throw new IllegalArgumentException("Invalid Object storage Uri: " + uri);
  }
}
