package com.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.S3Config;
import com.onehouse.config.configv1.ConfigV1;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@ExtendWith(MockitoExtension.class)
class S3AsyncClientProviderTest {

  @Mock private ConfigV1 config;

  @Mock private FileSystemConfiguration fileSystemConfiguration;

  @Mock private S3Config s3Config;

  @Mock private ExecutorService executorService;

  @Test
  void shouldThrowExceptionWhenS3ConfigIsNull() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(null);

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> new S3AsyncClientProvider(config, executorService));

    assertEquals("S3 Config not found", thrown.getMessage());
  }

  @Test
  void shouldThrowExceptionWhenRegionIsBlank() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    when(s3Config.getRegion()).thenReturn("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> new S3AsyncClientProvider(config, executorService));

    assertEquals("Aws region cannot be empty", thrown.getMessage());
  }

  @Test
  void shouldCreateS3AsyncClientWithCredentialsWhenProvided() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    when(s3Config.getRegion()).thenReturn("us-west-2");

    S3AsyncClientProvider s3AsyncClientProviderSpy =
        Mockito.spy(new S3AsyncClientProvider(config, executorService));

    S3AsyncClient s3AsyncClient = Mockito.mock(S3AsyncClient.class);

    doReturn(s3AsyncClient).when(s3AsyncClientProviderSpy).createS3AsyncClient();

    S3AsyncClient result = s3AsyncClientProviderSpy.getS3AsyncClient();

    assertSame(s3AsyncClient, result);
    verify(s3AsyncClientProviderSpy).createS3AsyncClient();
  }
}