package ai.onehouse.constants;

import java.util.regex.Pattern;

public class StorageConstants {
  private StorageConstants() {}

  // typical s3 path: "s3://bucket-name/path/to/object"
  // gcs path format: "gs://bucket/path/to/file"
  // azure blob format: "https://account.blob.core.windows.net/container/path/to/blob"
  // azure adls gen2 format: "https://account.dfs.core.windows.net/container/path/to/file"
  public static final Pattern OBJECT_STORAGE_URI_PATTERN =
      Pattern.compile("^(?:(s3://|gs://)|https://[^.]+\\.(?:blob|dfs)\\.core\\.windows\\.net/)([^/]+)(/.*)?$");

  // Azure-specific pattern to extract account name from URI
  // Group 1: account name, Group 2: container name, Group 3: blob path
  public static final Pattern AZURE_STORAGE_URI_PATTERN =
      Pattern.compile("^https://([^.]+)\\.(?:blob|dfs)\\.core\\.windows\\.net/([^/]+)(/.*)?$");

  // https://cloud.google.com/compute/docs/naming-resources#resource-name-format
  public static final String GCP_RESOURCE_NAME_FORMAT = "^[a-z]([-a-z0-9]*[a-z0-9])$";
}
