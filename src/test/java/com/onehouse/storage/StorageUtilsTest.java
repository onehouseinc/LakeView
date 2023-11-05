package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StorageUtilsTest {
  StorageUtils storageUtils = new StorageUtils();

  @Test
  void testGetPathFromUrl() {
    assertEquals("path/to/file", storageUtils.getPathFromUrl("s3://bucket/path/to/file"));
    assertEquals("path/to/file", storageUtils.getPathFromUrl("gs://bucket/path/to/file"));
    assertEquals("", storageUtils.getPathFromUrl("s3://bucket"));
    assertEquals("", storageUtils.getPathFromUrl("gs://bucket"));
    assertThrows(IllegalArgumentException.class, () -> storageUtils.getPathFromUrl("invalidUri"));
  }

  @Test
  void testGetBucketNameFromUri() {
    assertEquals("bucket", storageUtils.getBucketNameFromUri("s3://bucket/path/to/file"));
    assertEquals("bucket", storageUtils.getBucketNameFromUri("gs://bucket/path/to/file"));
    assertThrows(
        IllegalArgumentException.class, () -> storageUtils.getBucketNameFromUri("invalidUri"));
  }
}
