package com.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.cloud.storage.Storage;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.GCSConfig;
import com.onehouse.config.configv1.ConfigV1;
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
    when(gcsConfig.getProjectId()).thenReturn("valid-project-id");
    when(gcsConfig.getGcpServiceAccountKeyPath())
        .thenReturn("src/test/resources/dummyServiceAccountKey.json");

    GcsClientProvider gcsClientProviderSpy = Mockito.spy(new GcsClientProvider(config));

    doReturn(mockStorage).when(gcsClientProviderSpy).createGcsClient();

    // Assert
    assertNotNull(gcsClientProviderSpy.getGcsClient());
    verify(gcsClientProviderSpy).createGcsClient(); // Verify that createGcsClient was called
  }

  @Test
  void throwExceptionWhenGcsConfigIsNull() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(null);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> new GcsClientProvider(config));

    assertEquals("Gcs config not found", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenProjectIdIsInvalid() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);
    when(gcsConfig.getProjectId()).thenReturn("#invalid-project-id");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> new GcsClientProvider(config));

    assertEquals("Invalid GCP project ID: #invalid-project-id", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenServiceAccountKeyPathIsBlank() {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);
    when(gcsConfig.getProjectId())
        .thenReturn("valid-project-id"); // assuming it matches GCP_RESOURCE_NAME_FORMAT
    when(gcsConfig.getGcpServiceAccountKeyPath()).thenReturn("");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> new GcsClientProvider(config));

    assertEquals("Invalid GCP Service Account Key Path: ", thrown.getMessage());
  }
}
