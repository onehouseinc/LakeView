package ai.onehouse.storage;

import static ai.onehouse.constants.StorageConstants.AZURE_STORAGE_URI_PATTERN;
import static ai.onehouse.constants.StorageConstants.OBJECT_STORAGE_URI_PATTERN;

import java.util.regex.Matcher;

public class StorageUtils {
  private static final String INVALID_STORAGE_URI_ERROR_MSG = "Invalid Object storage Uri: ";

  public String getPathFromUrl(String uri) {

    if (!OBJECT_STORAGE_URI_PATTERN.matcher(uri).matches()) {
      throw new IllegalArgumentException(INVALID_STORAGE_URI_ERROR_MSG + uri);
    }

    Matcher matcher = OBJECT_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      String path = matcher.group(3);
      // Remove leading slash if present
      return (path != null && path.startsWith("/")) ? path.substring(1) : (path != null ? path : "");
    }

    return "";
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

  public String getAccountNameFromAzureUri(String uri) {
    Matcher matcher = AZURE_STORAGE_URI_PATTERN.matcher(uri);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid Azure storage Uri: " + uri);
  }

  public boolean isAzureUri(String uri) {
    return AZURE_STORAGE_URI_PATTERN.matcher(uri).matches();
  }
}
