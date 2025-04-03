package ai.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.GCSConfig;
import ai.onehouse.config.models.configv1.ConfigV1;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcsClientProviderTest {
  @Mock private ConfigV1 config;
  @Mock private FileSystemConfiguration fileSystemConfiguration;
  @Mock private GCSConfig gcsConfig;
  @Mock private Storage mockStorage;
  @Mock private GoogleCredentials googleCredentials;
  @Mock private Storage storage;

  @BeforeEach
  void setup() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateGcsClient(boolean isRefreshClient) {
    when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);

    GcsClientProvider gcsClientProviderSpy = Mockito.spy(new GcsClientProvider(config));

    doReturn(mockStorage).when(gcsClientProviderSpy).createGcsClient();

    // Assert
    if (!isRefreshClient) {
      assertNotNull(gcsClientProviderSpy.getGcsClient());
    } else {
      gcsClientProviderSpy.refreshClient();

    }
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateGcsClientWithValidConfig(boolean impersonatedCredentialsFlow) throws Exception {
    String serviceAccountKeyPath = "/path/to/key.json";
    String projectId = "your-project-id";
    try (MockedStatic<GoogleCredentials> credentialsMock = mockStatic(GoogleCredentials.class);
        MockedStatic<StorageOptions> optionsMock = mockStatic(StorageOptions.class);
        MockedStatic<ImpersonatedCredentials> impersonatedCredsMock = mockStatic(ImpersonatedCredentials.class)) {
      when(fileSystemConfiguration.getGcsConfig()).thenReturn(gcsConfig);
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      credentialsMock.when(() -> GoogleCredentials.fromStream(any())).thenReturn(mockCredentials);
      ImpersonatedCredentials impersonatedCredentials = mock(ImpersonatedCredentials.class);
      if (impersonatedCredentialsFlow) {
        impersonatedCredsMock.when(() ->
                ImpersonatedCredentials.create(mockCredentials, "test-sa", null,
                    Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"), 3600))
            .thenReturn(impersonatedCredentials);
      }

      GcsClientProvider gcsClientProviderSpy = Mockito.spy(new GcsClientProvider(config));

      StorageOptions.Builder builder = mock(StorageOptions.Builder.class);
      optionsMock.when(StorageOptions::newBuilder).thenReturn(builder);
      if (impersonatedCredentialsFlow) {
        when(builder.setCredentials(impersonatedCredentials)).thenReturn(builder);
      } else {
        when(builder.setCredentials(mockCredentials)).thenReturn(builder);
      }
      when(builder.setProjectId(projectId)).thenReturn(builder);

      StorageOptions options = mock(StorageOptions.class);
      when(builder.build()).thenReturn(options);

      FileInputStream fileInputStream = mock(FileInputStream.class);
      doReturn(fileInputStream).when(gcsClientProviderSpy).readAsStream();

      when(options.getService()).thenReturn(storage);

      // Set GcsConfig behavior
      when(gcsConfig.getGcpServiceAccountKeyPath()).thenReturn(Optional.of(serviceAccountKeyPath));
      when(gcsConfig.getProjectId()).thenReturn(Optional.of(projectId));
      if (impersonatedCredentialsFlow) {
        when(gcsConfig.getServiceAccountToImpersonate()).thenReturn(Optional.of("test-sa"));
      }

      // Create instance of the class to test
      Storage result = gcsClientProviderSpy.createGcsClient();

      assertNotNull(result);
      verify(gcsClientProviderSpy, times(1)).readAsStream();
    }
  }

  @Test
  void testCreateGcsClientWithNullConfig() {
    try (MockedStatic<StorageOptions> optionsMock = mockStatic(StorageOptions.class)) {
      when(fileSystemConfiguration.getGcsConfig()).thenReturn(null);

      GcsClientProvider gcsClientProviderSpy = Mockito.spy(new GcsClientProvider(config));

      StorageOptions options = mock(StorageOptions.class);
      when(options.getService()).thenReturn(storage);
      optionsMock.when(StorageOptions::getDefaultInstance).thenReturn(options);

      Storage result = gcsClientProviderSpy.createGcsClient();

      assertNotNull(result);
    }
  }
}
