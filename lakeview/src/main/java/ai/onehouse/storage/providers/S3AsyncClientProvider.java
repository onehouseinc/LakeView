package ai.onehouse.storage.providers;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.S3Config;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

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

    if (s3Config.getAccessKey().isPresent() && s3Config.getAccessSecret().isPresent()) {
      logger.debug("Using provided accessKey and accessSecret for authentication");
      AwsBasicCredentials awsCredentials =
        AwsBasicCredentials.create(
          s3Config.getAccessKey().get(), s3Config.getAccessSecret().get());
      s3AsyncClientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    } else if (s3Config.getArnToImpersonate().isPresent()) {
      // Assume role of Destination ARN
      try (StsClient stsClient = StsClient.builder()
        .region(Region.of(s3Config.getRegion()))
        .build()) {
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
          .roleArn(s3Config.getArnToImpersonate().get())
          .roleSessionName(String.format("S3AsyncClientSession-%s", extractAccountIdFromArn(s3Config.getArnToImpersonate().get())))
          .build();
        AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);
        AwsSessionCredentials tempCredentials = AwsSessionCredentials.create(
          assumeRoleResponse.credentials().accessKeyId(),
          assumeRoleResponse.credentials().secretAccessKey(),
          assumeRoleResponse.credentials().sessionToken()
        );
        s3AsyncClientBuilder.credentialsProvider(StaticCredentialsProvider.create(tempCredentials));
      }
    }

    return s3AsyncClientBuilder
      .httpClient(NettyNioAsyncHttpClient.builder()
        .maxConcurrency(200)
        .connectionTimeout(Duration.ofSeconds(60L))
        .build())
      .region(Region.of(s3Config.getRegion()))
      .asyncConfiguration(
        builder ->
          builder.advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executorService))
        .build();
  }

  private static String extractAccountIdFromArn(String arn) {
    Matcher matcher = Pattern.compile("arn:aws:iam::(\\d+):role/").matcher(arn);
    return matcher.find() ? matcher.group(1) : "";
  }

  public S3AsyncClient getS3AsyncClient() {
    if (s3AsyncClient == null) {
      s3AsyncClient = createS3AsyncClient();
    }
    return s3AsyncClient;
  }

  public void refreshClient() {
    s3AsyncClient = createS3AsyncClient();
  }

  private void validateS3Config(S3Config s3Config) {
    if (s3Config == null) {
      throw new IllegalArgumentException("S3 Config not found");
    }

    if (StringUtils.isBlank(s3Config.getRegion())) {
      throw new IllegalArgumentException("Aws region cannot be empty");
    }
  }

  @VisibleForTesting
  static void resetS3AsyncClient() {
    s3AsyncClient = null;
  }
}
