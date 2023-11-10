package com.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.cloud.storage.Storage;
import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.GCSConfig;
import com.onehouse.config.models.configv1.ConfigV1;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcsClientProviderTest {
  @Mock private ConfigV1 config;
  @Mock private FileSystemConfiguration fileSystemConfiguration;
  @Mock private GCSConfig gcsConfig;
  @Mock private Storage mockStorage;

  @BeforeEach
  void setup() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
  }

  @Test
  void testInstantiateGcsClient() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);

    GcsClientProvider gcsClientProviderSpy = Mockito.spy(new GcsClientProvider(config));

    doReturn(mockStorage).when(gcsClientProviderSpy).createGcsClient();

    // Assert
    assertNotNull(gcsClientProviderSpy.getGcsClient());
    verify(gcsClientProviderSpy, times(1))
        .createGcsClient(); // Verify that createGcsClient was called
  }

  @Test
  void throwExceptionWhenProjectIdIsInvalid() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);
    when(gcsConfig.getProjectId()).thenReturn(Optional.of("#invalid-project-id"));
    GcsClientProvider clientProvider = new GcsClientProvider(config);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::getGcsClient);

    assertEquals("Invalid GCP project ID: #invalid-project-id", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenServiceAccountKeyPathIsBlank() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);
    when(gcsConfig.getProjectId()).thenReturn(Optional.of("valid-project-id"));
    when(gcsConfig.getGcpServiceAccountKeyPath()).thenReturn(Optional.of(""));
    GcsClientProvider clientProvider = new GcsClientProvider(config);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::getGcsClient);

    assertEquals("Invalid GCP Service Account Key Path: ", thrown.getMessage());
  }
}
