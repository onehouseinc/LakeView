package ai.onehouse.storage;

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
  void testConstructFileUri() {
    String s3DirUriWithoutTrailingSlash = "s3://bucket/dir1";
    String s3DirUriWithTrailingSlash = "s3://bucket/dir1/";
    String filePathWithoutPrefixSlash = "file.txt";
    String filePathWithPrefixSlash = "/file.txt";
    String expectedFileUri = s3DirUriWithTrailingSlash + filePathWithoutPrefixSlash;
    assertEquals(
        expectedFileUri,
        storageUtils.constructFileUri(s3DirUriWithoutTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedFileUri,
        storageUtils.constructFileUri(s3DirUriWithTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedFileUri,
        storageUtils.constructFileUri(s3DirUriWithoutTrailingSlash, filePathWithPrefixSlash));
    assertEquals(
        expectedFileUri,
        storageUtils.constructFileUri(s3DirUriWithTrailingSlash, filePathWithPrefixSlash));
    assertEquals(
        filePathWithPrefixSlash, storageUtils.constructFileUri("", filePathWithoutPrefixSlash));
    assertEquals(
        filePathWithPrefixSlash, storageUtils.constructFileUri("", filePathWithPrefixSlash));
    assertEquals(
        s3DirUriWithTrailingSlash, storageUtils.constructFileUri(s3DirUriWithTrailingSlash, ""));
    assertEquals(
        s3DirUriWithTrailingSlash, storageUtils.constructFileUri(s3DirUriWithoutTrailingSlash, ""));
  }

  @Test
  void testGetBucketNameFromUri() {
    assertEquals("bucket", storageUtils.getBucketNameFromUri("s3://bucket/path/to/file"));
    assertEquals("bucket", storageUtils.getBucketNameFromUri("gs://bucket/path/to/file"));
    assertThrows(
        IllegalArgumentException.class, () -> storageUtils.getBucketNameFromUri("invalidUri"));
  }
}
