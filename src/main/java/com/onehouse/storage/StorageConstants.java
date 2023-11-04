package com.onehouse.storage;

import java.util.regex.Pattern;

public class StorageConstants {
  // typical s3 path: "s3://bucket-name/path/to/object"
  // gcs path format "gs:// [bucket] /path/to/file"
  public static final Pattern OBJECT_STORAGE_URI_PATTERN =
      Pattern.compile("^(s3://|gs://)([^/]+)(/.*)?");

  // https://cloud.google.com/compute/docs/naming-resources#resource-name-format
  public static final String GCP_RESOURCE_NAME_FORMAT = "^[a-z]([-a-z0-9]*[a-z0-9])$";
}
