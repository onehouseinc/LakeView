package ai.onehouse.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StorageUtilsTest {
  StorageUtils storageUtils = new StorageUtils();

  @Test
  void testGetPathFromUrl() {
    assertEquals("path/to/file", storageUtils.getPathFromUrl("s3://bucket/path/to/file"));
    assertEquals("path/to/file", storageUtils.getPathFromUrl("gs://bucket/path/to/file"));
    assertEquals(
        "path/to/file",
        storageUtils.getPathFromUrl(
            "https://account.blob.core.windows.net/container/path/to/file"));
    assertEquals(
        "path/to/file",
        storageUtils.getPathFromUrl(
            "https://account.dfs.core.windows.net/container/path/to/file"));
    assertEquals(
        "path/to/file",
        storageUtils.getPathFromUrl(
            "abfss://container@account.dfs.core.windows.net/path/to/file"));
    assertEquals("", storageUtils.getPathFromUrl("s3://bucket"));
    assertEquals("", storageUtils.getPathFromUrl("gs://bucket"));
    assertEquals(
        "", storageUtils.getPathFromUrl("https://account.blob.core.windows.net/container"));
    assertEquals(
        "", storageUtils.getPathFromUrl("https://account.dfs.core.windows.net/container"));
    assertEquals(
        "", storageUtils.getPathFromUrl("abfss://container@account.dfs.core.windows.net"));
    assertThrows(IllegalArgumentException.class, () -> storageUtils.getPathFromUrl("invalidUri"));
  }

  @Test
  void testConstructFileUri() {
    String s3DirUriWithoutTrailingSlash = "s3://bucket/dir1";
    String s3DirUriWithTrailingSlash = "s3://bucket/dir1/";
    String azureDirUriWithoutTrailingSlash =
        "https://account.blob.core.windows.net/container/dir1";
    String azureDirUriWithTrailingSlash =
        "https://account.blob.core.windows.net/container/dir1/";
    String abfssDirUriWithoutTrailingSlash =
        "abfss://container@account.dfs.core.windows.net/dir1";
    String abfssDirUriWithTrailingSlash =
        "abfss://container@account.dfs.core.windows.net/dir1/";
    String filePathWithoutPrefixSlash = "file.txt";
    String filePathWithPrefixSlash = "/file.txt";
    String expectedS3FileUri = s3DirUriWithTrailingSlash + filePathWithoutPrefixSlash;
    String expectedAzureFileUri = azureDirUriWithTrailingSlash + filePathWithoutPrefixSlash;
    String expectedAbfssFileUri = abfssDirUriWithTrailingSlash + filePathWithoutPrefixSlash;

    // S3 tests
    assertEquals(
        expectedS3FileUri,
        storageUtils.constructFileUri(s3DirUriWithoutTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedS3FileUri,
        storageUtils.constructFileUri(s3DirUriWithTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedS3FileUri,
        storageUtils.constructFileUri(s3DirUriWithoutTrailingSlash, filePathWithPrefixSlash));
    assertEquals(
        expectedS3FileUri,
        storageUtils.constructFileUri(s3DirUriWithTrailingSlash, filePathWithPrefixSlash));

    // Azure tests
    assertEquals(
        expectedAzureFileUri,
        storageUtils.constructFileUri(
            azureDirUriWithoutTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedAzureFileUri,
        storageUtils.constructFileUri(azureDirUriWithTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedAzureFileUri,
        storageUtils.constructFileUri(azureDirUriWithoutTrailingSlash, filePathWithPrefixSlash));
    assertEquals(
        expectedAzureFileUri,
        storageUtils.constructFileUri(azureDirUriWithTrailingSlash, filePathWithPrefixSlash));

    // ABFSS tests
    assertEquals(
        expectedAbfssFileUri,
        storageUtils.constructFileUri(
            abfssDirUriWithoutTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedAbfssFileUri,
        storageUtils.constructFileUri(abfssDirUriWithTrailingSlash, filePathWithoutPrefixSlash));
    assertEquals(
        expectedAbfssFileUri,
        storageUtils.constructFileUri(abfssDirUriWithoutTrailingSlash, filePathWithPrefixSlash));
    assertEquals(
        expectedAbfssFileUri,
        storageUtils.constructFileUri(abfssDirUriWithTrailingSlash, filePathWithPrefixSlash));

    // Edge cases
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
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri(
            "https://account.blob.core.windows.net/container/path/to/file"));
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri(
            "https://account.dfs.core.windows.net/container/path/to/file"));
    assertEquals(
        "onehouse-customer-bucket-ed97feae",
        storageUtils.getBucketNameFromUri(
            "abfss://onehouse-customer-bucket-ed97feae@storage1houseed97feae.dfs.core.windows.net/internal-integration-test-0/kafka_event_test_98def769_1774259480797/kafka_event_topic_custom_transformer_98def769/v1"));
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri(
            "abfss://container@account.dfs.core.windows.net/path/to/file"));
    assertEquals("bucket", storageUtils.getBucketNameFromUri("s3://bucket"));
    assertEquals("bucket", storageUtils.getBucketNameFromUri("gs://bucket"));
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri("https://account.blob.core.windows.net/container"));
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri("https://account.dfs.core.windows.net/container"));
    assertEquals(
        "container",
        storageUtils.getBucketNameFromUri("abfss://container@account.dfs.core.windows.net"));
    assertThrows(
        IllegalArgumentException.class, () -> storageUtils.getBucketNameFromUri("invalidUri"));
  }
}
