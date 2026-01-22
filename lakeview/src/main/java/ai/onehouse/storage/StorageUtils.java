package ai.onehouse.storage;

import org.apache.commons.lang3.StringUtils;

import static ai.onehouse.constants.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  private static final String INVALID_STORAGE_URI_ERROR_MSG = "Invalid Object storage Uri: ";

  public String getPathFromUrl(String uri) {

    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
    }

    /*
     * Group 3 extracts the path portion after the bucket/container name from the URI.
     * Examples:
     *   s3://my-bucket/path/to/file.txt -> group(3) = /path/to/file.txt
     *   gs://my-bucket/path/to/file.txt -> group(3) = /path/to/file.txt
     *   https://account.blob.core.windows.net/container/path/to/file.txt -> group(3) = /path/to/file.txt
     *   https://account.dfs.core.windows.net/container/path/to/file.txt -> group(3) = /path/to/file.txt
     */
    String path = matcher.group(3);
    return path == null ? StringUtils.EMPTY : path.replaceFirst("^/", "");
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
      return matcher.group(2);
    }
    throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
  }
}
