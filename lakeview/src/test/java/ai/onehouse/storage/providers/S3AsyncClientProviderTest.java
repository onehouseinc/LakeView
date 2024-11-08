package ai.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.S3Config;
import ai.onehouse.config.models.configv1.ConfigV1;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

@ExtendWith(MockitoExtension.class)
class S3AsyncClientProviderTest {
  @Mock private ConfigV1 config;
  @Mock private FileSystemConfiguration fileSystemConfiguration;
  @Mock private S3Config s3Config;
  @Mock private ExecutorService executorService;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  StsClient stsClient;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  StsClientBuilder stsClientBuilder;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  AssumeRoleResponse assumeRoleResponse;

  @AfterEach
  void clearCaches() {
    clearInvocations();
  }

  @Test
  void throwExceptionWhenS3ConfigIsNull() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(null);
    S3AsyncClientProvider clientProvider = new S3AsyncClientProvider(config, executorService);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createS3AsyncClient);

    assertEquals("S3 Config not found", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenRegionIsBlank() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    when(s3Config.getRegion()).thenReturn("");

    S3AsyncClientProvider clientProvider = new S3AsyncClientProvider(config, executorService);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createS3AsyncClient);

    assertEquals("Aws region cannot be empty", thrown.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateS3AsyncClientWithCredentialsWhenProvided(boolean isAssumedRoleFlow) {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    if (isAssumedRoleFlow) {
      when(s3Config.getDestinationArn()).thenReturn(Optional.of("arn:aws:iam::396675327081:role/test-role"));
    } else {
      when(s3Config.getAccessSecret()).thenReturn(Optional.of("access-secret"));
      when(s3Config.getAccessKey()).thenReturn(Optional.of("access-key"));
    }
    when(s3Config.getRegion()).thenReturn("us-west-2");

    try (MockedStatic<StsClient> mockedStatic = Mockito.mockStatic(StsClient.class)) {
      if (isAssumedRoleFlow) {
        mockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
        when(StsClient.builder().region(any()).build()).thenReturn(stsClient);
        when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);
        when(assumeRoleResponse.credentials().accessKeyId()).thenReturn("access-key");
        when(assumeRoleResponse.credentials().secretAccessKey()).thenReturn("access-secret");
        when(assumeRoleResponse.credentials().sessionToken()).thenReturn("session-token");
      }

      S3AsyncClientProvider s3AsyncClientProviderSpy =
          Mockito.spy(new S3AsyncClientProvider(config, executorService));
      S3AsyncClientProvider.resetS3AsyncClient();
      S3AsyncClient result = s3AsyncClientProviderSpy.getS3AsyncClient();

      assertNotNull(result);
      if (isAssumedRoleFlow) {
        verify(stsClient, times(1)).assumeRole(any(AssumeRoleRequest.class));
      }
      verify(s3AsyncClientProviderSpy, times(1)).createS3AsyncClient();
    }
  }
}
