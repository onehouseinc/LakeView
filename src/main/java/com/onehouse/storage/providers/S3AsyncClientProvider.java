package com.onehouse.storage.providers;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigV1;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.S3Config;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

@Getter
public class S3AsyncClientProvider {
  private final S3AsyncClient s3AsyncClient;

  @Inject
  public S3AsyncClientProvider(@Nonnull Config config, @Nonnull ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    validateS3Config(fileSystemConfiguration.getS3Config());

    S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder();

    if (!fileSystemConfiguration.getS3Config().getAccessKey().isBlank()
        && !fileSystemConfiguration.getS3Config().getAccessSecret().isBlank()) {
      AwsBasicCredentials awsCredentials =
          AwsBasicCredentials.create(
              fileSystemConfiguration.getS3Config().getAccessKey(),
              fileSystemConfiguration.getS3Config().getAccessSecret());
      s3AsyncClientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    }

    this.s3AsyncClient =
        s3AsyncClientBuilder
            .region(Region.of(fileSystemConfiguration.getS3Config().getRegion()))
            .asyncConfiguration(
                b ->
                    b.advancedOption(
                        SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executorService))
            .build();
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
