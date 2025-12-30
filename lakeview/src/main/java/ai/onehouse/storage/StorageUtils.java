package ai.onehouse.storage;

import static ai.onehouse.constants.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  private static final String INVALID_STORAGE_URI_ERROR_MSG = "Invalid Object storage Uri: ";

  public String getPathFromUrl(String uri) {

    if (!OBJECT_STORAGE_URI_PATTERN.matcher(uri).matches()) {
      throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
    }

    String prefix = "";

    // Handle Azure blob URLs: https://storageaccount.blob.core.windows.net/container/path
    if (uri.startsWith("https://") && uri.contains(".blob.core.windows.net/")) {
      int containerStart = uri.indexOf(".blob.core.windows.net/") + ".blob.core.windows.net/".length();
      int pathStart = uri.indexOf('/', containerStart);
      if (pathStart != -1) {
        prefix = uri.substring(pathStart + 1);
      }
    } else {
      // Remove the scheme and bucket name from the S3/GCS path
      int startIndex = uri.indexOf('/', 5); // Skip 's3://' and 'gs://'
      if (startIndex != -1) {
        prefix = uri.substring(startIndex + 1);
      }
    }

    return prefix;
  }

  public String constructFileUri(String directoryUri, String filePath) {
    return String.format(
        "%s/%s",
        directoryUri.endsWith("/")
            ? directoryUri.substring(0, directoryUri.length() - 1)
            : directoryUri,
        filePath.startsWith("/") ? filePath.substring(1) : filePath);
  }

  public String getBucketNameFromUri(String uri) {
    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      // Handle Azure blob URLs: group 7 is the container name
      if (uri.startsWith("https://") && uri.contains(".blob.core.windows.net/")) {
        String container = matcher.group(7);
        if (container != null) {
          return container;
        }
      } else {
        // S3/GCS: group 3 is the bucket name
        return matcher.group(3);
      }
    }
    throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
  }
  
  public String getStorageAccountFromUri(String uri) {
    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches() && uri.startsWith("https://") && uri.contains(".blob.core.windows.net/")) {
      String storageAccount = matcher.group(6);
      if (storageAccount != null) {
        return storageAccount;
      }
    }
    throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
  }
}
