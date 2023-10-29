package com.onehouse.storage;

import static com.onehouse.storage.StorageConstants.S3_PATH_PATTERN;

import java.util.regex.Matcher;

public class S3Utils {
  public String getPathFromS3Url(String s3Path) {
    String prefix = null;

    // Remove the scheme and bucket name from the S3 path
    int startIndex = s3Path.indexOf('/', 5); // Skip 's3://'
    if (startIndex != -1) {
      prefix = s3Path.substring(startIndex + 1);
    }
    // Ensure the prefix ends with a '/'
    if (!prefix.endsWith("/")) {
      prefix = prefix + "/";
    }

    return prefix;
  }

  public String getS3BucketNameFromS3Url(String s3Path) {
    Matcher matcher = S3_PATH_PATTERN.matcher(s3Path);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid AWS S3 path: " + s3Path);
  }
}
