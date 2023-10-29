package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigV1;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.S3Config;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nonnull;

public class S3AsyncStorageLister implements AsyncStorageLister {
  private final S3AsyncClient s3AsyncClient;
  // typical s3 path: "s3://bucket-name/path/to/object"
  private static final Pattern S3_PATH_PATTERN = Pattern.compile("^s3://([^/]+)/.*");

  @Inject
  public S3AsyncStorageLister(@Nonnull Config config, @Nonnull ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    validateS3Config(fileSystemConfiguration.getS3Config());
    // TODO: support accesskey based auth if provided
    this.s3AsyncClient =
        S3AsyncClient.builder()
            .region(Region.of(fileSystemConfiguration.getS3Config().getRegion()))
            .asyncConfiguration(
                b ->
                    b.advancedOption(
                        SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executorService))
            .build();
  }

  @Override
  public CompletableFuture<List<String>> listFiles(String s3path) {
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(getS3BucketNameFromPath(s3path))
            .prefix(getPrefix(s3path))
            .build();
    return s3AsyncClient
        .listObjectsV2(listObjectsV2Request)
        .thenApply(
            listObjectsV2Response ->
                listObjectsV2Response.contents().stream()
                    .map(S3Object::key)
                    .collect(Collectors.toList()));
  }

  /*
   * Extracts the prefix from a given S3 path.
   */
  public static String getPrefix(String s3Path) {
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

  private String getS3BucketNameFromPath(String s3Path) {
    Matcher matcher = S3_PATH_PATTERN.matcher(s3Path);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid AWS S3 path: " + s3Path);
  }

  private void validateS3Config(S3Config s3Config) {
    if (s3Config == null) {
      throw new IllegalArgumentException("S3 Config not found");
    }

    if (!s3Config.getRegion().isBlank()) {
      throw new IllegalArgumentException("Aws region cannot be empty");
    }
  }
}
