package ai.onehouse.storage;

import org.apache.commons.lang3.StringUtils;

import static ai.onehouse.constants.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  private static final String INVALID_STORAGE_URI_ERROR_MSG = "Invalid Object storage Uri: ";

  /**
   * Group 3 extracts the path portion after the bucket/container name from the URI.
   * Examples:
   * <ul>
   *   <li>s3://my-bucket/path/to/file.txt returns /path/to/file.txt</li>
   *   <li>gs://my-bucket/path/to/file.txt returns /path/to/file.txt</li>
   *   <li>https://account.blob.core.windows.net/container/path/to/file.txt returns /path/to/file.txt</li>
   *   <li>https://account.dfs.core.windows.net/container/path/to/file.txt returns /path/to/file.txt</li>
   *   <li>abfss://container@account.dfs.core.windows.net/path/to/file.txt returns /path/to/file.txt</li>
   * </ul>
   * @param uri the storage URI to parse
   * @return the path portion of the URI, or empty string if no path
   */
  public String getPathFromUrl(String uri) {

    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
    }

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

  /**
   * Group 2 extracts the bucket/container name from the URI.
   * Examples:
   * <ul>
   *   <li>s3://my-bucket-s3/path/to/file.txt returns my-bucket-s3</li>
   *   <li>gs://my-bucket-gs/path/to/file.txt returns my-bucket-gs</li>
   *   <li>https://account.blob.core.windows.net/container/path/file.txt returns container</li>
   *   <li>https://account.dfs.core.windows.net/container/path/file.txt returns container</li>
   *   <li>abfss://container@account.dfs.core.windows.net/path/file.txt returns container</li>
   * </ul>
   * @param uri the storage URI to parse
   * @return the bucket or container name
   */
  public String getBucketNameFromUri(String uri) {
    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
  }
}
