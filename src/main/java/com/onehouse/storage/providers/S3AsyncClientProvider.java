package com.onehouse.storage.providers;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.S3Config;
import com.onehouse.config.configV1.ConfigV1;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

@Getter
public class S3AsyncClientProvider {
  private final S3AsyncClient s3AsyncClient;
  private static final Logger logger = LoggerFactory.getLogger(S3AsyncClientProvider.class);

  @Inject
  public S3AsyncClientProvider(@Nonnull Config config, @Nonnull ExecutorService executorService) {
    logger.debug("Instantiating S3 storage client");
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    validateS3Config(fileSystemConfiguration.getS3Config());

    S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder();

    if (fileSystemConfiguration.getS3Config().getAccessKey().isPresent()
        && fileSystemConfiguration.getS3Config().getAccessSecret().isPresent()) {
      logger.debug("Using provided accessKey and accessSecret for authentication");
      AwsBasicCredentials awsCredentials =
          AwsBasicCredentials.create(
              fileSystemConfiguration.getS3Config().getAccessKey().get(),
              fileSystemConfiguration.getS3Config().getAccessSecret().get());
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

    if (s3Config.getRegion().isBlank()) {
      throw new IllegalArgumentException("Aws region cannot be empty");
    }
  }
}
