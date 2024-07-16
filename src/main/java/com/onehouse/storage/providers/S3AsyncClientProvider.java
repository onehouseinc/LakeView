package com.onehouse.storage.providers;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.S3Config;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

public class S3AsyncClientProvider {
  private final S3Config s3Config;
  private final ExecutorService executorService;
  private static S3AsyncClient s3AsyncClient;
  private static final Logger logger = LoggerFactory.getLogger(S3AsyncClientProvider.class);

  @Inject
  public S3AsyncClientProvider(@Nonnull Config config, @Nonnull ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    this.s3Config = fileSystemConfiguration.getS3Config();
    this.executorService = executorService;
  }

  protected S3AsyncClient createS3AsyncClient() {
    logger.debug("Instantiating S3 storage client");
    validateS3Config(s3Config);
    S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder();

    if (s3Config.getEndpoint().isPresent()) {
      logger.debug("Using provided endpoint");
      s3AsyncClientBuilder.endpointOverride(URI.create(s3Config.getEndpoint().get()));
    }

    if (s3Config.getForcePathStyle().isPresent()) {
      logger.debug("Using provided forcePathStyle");
      s3AsyncClientBuilder.forcePathStyle(s3Config.getForcePathStyle().get());
    }

    if (s3Config.getAccessKey().isPresent() && s3Config.getAccessSecret().isPresent()) {
      logger.debug("Using provided accessKey and accessSecret for authentication");
      AwsBasicCredentials awsCredentials =
          AwsBasicCredentials.create(
              s3Config.getAccessKey().get(), s3Config.getAccessSecret().get());
      s3AsyncClientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    }

    return s3AsyncClientBuilder
        .region(Region.of(s3Config.getRegion()))
        .asyncConfiguration(
            builder ->
                builder.advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executorService))
        .build();
  }

  public S3AsyncClient getS3AsyncClient() {
    if (s3AsyncClient == null) {
      s3AsyncClient = createS3AsyncClient();
    }
    return s3AsyncClient;
  }

  private void validateS3Config(S3Config s3Config) {
    if (s3Config == null) {
      throw new IllegalArgumentException("S3 Config not found");
    }

    if (StringUtils.isBlank(s3Config.getRegion())) {
      throw new IllegalArgumentException("Aws region cannot be empty");
    }
  }
}
