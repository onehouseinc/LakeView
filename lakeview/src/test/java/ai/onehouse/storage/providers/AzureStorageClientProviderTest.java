package ai.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import ai.onehouse.config.models.common.AzureConfig;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.configv1.ConfigV1;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AzureStorageClientProviderTest {
  @Mock private ConfigV1 config;
  @Mock private FileSystemConfiguration fileSystemConfiguration;
  @Mock private AzureConfig azureConfig;
  @Mock private DataLakeServiceAsyncClient mockDataLakeServiceAsyncClient;

  @BeforeEach
  void setup() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getAzureConfig()).thenReturn(azureConfig);
  }

  @Test
  void throwExceptionWhenAzureConfigIsNull() {
    when(fileSystemConfiguration.getAzureConfig()).thenReturn(null);
    AzureStorageClientProvider clientProvider = new AzureStorageClientProvider(config);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createAzureAsyncClient);

    assertEquals("Azure Config not found", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenAccountNameIsBlank() {
    when(azureConfig.getAccountName()).thenReturn("");

    AzureStorageClientProvider clientProvider = new AzureStorageClientProvider(config);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createAzureAsyncClient);

    assertEquals("Azure storage account name cannot be empty", thrown.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateAzureClientWithConnectionString(boolean isRefreshClient) {
    when(azureConfig.getAccountName()).thenReturn("testaccount");
    when(azureConfig.getConnectionString())
        .thenReturn(
            Optional.of(
                "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=key;EndpointSuffix=core.windows.net"));

    try (MockedConstruction<DataLakeServiceClientBuilder> mockedBuilder =
        mockConstruction(
            DataLakeServiceClientBuilder.class,
            (mock, context) -> {
              when(mock.endpoint(anyString())).thenReturn(mock);
              when(mock.connectionString(anyString())).thenReturn(mock);
              when(mock.buildAsyncClient()).thenReturn(mockDataLakeServiceAsyncClient);
            })) {

      AzureStorageClientProvider azureClientProviderSpy =
          Mockito.spy(new AzureStorageClientProvider(config));
      AzureStorageClientProvider.resetAzureAsyncClient();

      if (!isRefreshClient) {
        DataLakeServiceAsyncClient result = azureClientProviderSpy.getAzureAsyncClient();
        assertNotNull(result);
      } else {
        azureClientProviderSpy.refreshClient();
      }

      verify(azureClientProviderSpy, times(1)).createAzureAsyncClient();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateAzureClientWithAccountKey(boolean isRefreshClient) {
    when(azureConfig.getAccountName()).thenReturn("testaccount");
    when(azureConfig.getConnectionString()).thenReturn(Optional.empty());
    when(azureConfig.getAccountKey()).thenReturn(Optional.of("dGVzdGFjY291bnRrZXk="));

    try (MockedConstruction<DataLakeServiceClientBuilder> mockedBuilder =
            mockConstruction(
                DataLakeServiceClientBuilder.class,
                (mock, context) -> {
                  when(mock.endpoint(anyString())).thenReturn(mock);
                  when(mock.credential(any(StorageSharedKeyCredential.class))).thenReturn(mock);
                  when(mock.buildAsyncClient()).thenReturn(mockDataLakeServiceAsyncClient);
                });
        MockedConstruction<StorageSharedKeyCredential> mockedCredential =
            mockConstruction(StorageSharedKeyCredential.class)) {

      AzureStorageClientProvider azureClientProviderSpy =
          Mockito.spy(new AzureStorageClientProvider(config));
      AzureStorageClientProvider.resetAzureAsyncClient();

      if (!isRefreshClient) {
        DataLakeServiceAsyncClient result = azureClientProviderSpy.getAzureAsyncClient();
        assertNotNull(result);
      } else {
        azureClientProviderSpy.refreshClient();
      }

      verify(azureClientProviderSpy, times(1)).createAzureAsyncClient();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateAzureClientWithServicePrincipal(boolean isRefreshClient) {
    when(azureConfig.getAccountName()).thenReturn("testaccount");
    when(azureConfig.getConnectionString()).thenReturn(Optional.empty());
    when(azureConfig.getAccountKey()).thenReturn(Optional.empty());
    when(azureConfig.getTenantId()).thenReturn(Optional.of("test-tenant-id"));
    when(azureConfig.getClientId()).thenReturn(Optional.of("test-client-id"));
    when(azureConfig.getClientSecret()).thenReturn(Optional.of("test-client-secret"));

    try (MockedConstruction<DataLakeServiceClientBuilder> mockedBuilder =
            mockConstruction(
                DataLakeServiceClientBuilder.class,
                (mock, context) -> {
                  when(mock.endpoint(anyString())).thenReturn(mock);
                  when(mock.credential(any(ClientSecretCredential.class))).thenReturn(mock);
                  when(mock.buildAsyncClient()).thenReturn(mockDataLakeServiceAsyncClient);
                });
        MockedConstruction<ClientSecretCredentialBuilder> mockedCredBuilder =
            mockConstruction(
                ClientSecretCredentialBuilder.class,
                (mock, context) -> {
                  when(mock.tenantId(anyString())).thenReturn(mock);
                  when(mock.clientId(anyString())).thenReturn(mock);
                  when(mock.clientSecret(anyString())).thenReturn(mock);
                  when(mock.build()).thenReturn(Mockito.mock(ClientSecretCredential.class));
                })) {

      AzureStorageClientProvider azureClientProviderSpy =
          Mockito.spy(new AzureStorageClientProvider(config));
      AzureStorageClientProvider.resetAzureAsyncClient();

      if (!isRefreshClient) {
        DataLakeServiceAsyncClient result = azureClientProviderSpy.getAzureAsyncClient();
        assertNotNull(result);
      } else {
        azureClientProviderSpy.refreshClient();
      }

      verify(azureClientProviderSpy, times(1)).createAzureAsyncClient();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateAzureClientWithManagedIdentity(boolean isRefreshClient) {
    when(azureConfig.getAccountName()).thenReturn("testaccount");
    when(azureConfig.getConnectionString()).thenReturn(Optional.empty());
    when(azureConfig.getAccountKey()).thenReturn(Optional.empty());
    when(azureConfig.getTenantId()).thenReturn(Optional.of("test-tenant-id"));
    when(azureConfig.getClientId()).thenReturn(Optional.of("test-client-id"));
    when(azureConfig.getClientSecret()).thenReturn(Optional.empty());

    try (MockedConstruction<DataLakeServiceClientBuilder> mockedBuilder =
            mockConstruction(
                DataLakeServiceClientBuilder.class,
                (mock, context) -> {
                  when(mock.endpoint(anyString())).thenReturn(mock);
                  when(mock.credential(any(DefaultAzureCredential.class))).thenReturn(mock);
                  when(mock.buildAsyncClient()).thenReturn(mockDataLakeServiceAsyncClient);
                });
        MockedConstruction<DefaultAzureCredentialBuilder> mockedCredBuilder =
            mockConstruction(
                DefaultAzureCredentialBuilder.class,
                (mock, context) -> {
                  when(mock.tenantId(anyString())).thenReturn(mock);
                  when(mock.managedIdentityClientId(anyString())).thenReturn(mock);
                  when(mock.build()).thenReturn(Mockito.mock(DefaultAzureCredential.class));
                })) {

      AzureStorageClientProvider azureClientProviderSpy =
          Mockito.spy(new AzureStorageClientProvider(config));
      AzureStorageClientProvider.resetAzureAsyncClient();

      if (!isRefreshClient) {
        DataLakeServiceAsyncClient result = azureClientProviderSpy.getAzureAsyncClient();
        assertNotNull(result);
      } else {
        azureClientProviderSpy.refreshClient();
      }

      verify(azureClientProviderSpy, times(1)).createAzureAsyncClient();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantiateAzureClientWithDefaultCredential(boolean isRefreshClient) {
    when(azureConfig.getAccountName()).thenReturn("testaccount");
    when(azureConfig.getConnectionString()).thenReturn(Optional.empty());
    when(azureConfig.getAccountKey()).thenReturn(Optional.empty());
    when(azureConfig.getTenantId()).thenReturn(Optional.empty());
    when(azureConfig.getClientId()).thenReturn(Optional.empty());

    try (MockedConstruction<DataLakeServiceClientBuilder> mockedBuilder =
            mockConstruction(
                DataLakeServiceClientBuilder.class,
                (mock, context) -> {
                  when(mock.endpoint(anyString())).thenReturn(mock);
                  when(mock.credential(any(DefaultAzureCredential.class))).thenReturn(mock);
                  when(mock.buildAsyncClient()).thenReturn(mockDataLakeServiceAsyncClient);
                });
        MockedConstruction<DefaultAzureCredentialBuilder> mockedCredBuilder =
            mockConstruction(
                DefaultAzureCredentialBuilder.class,
                (mock, context) -> {
                  when(mock.build()).thenReturn(Mockito.mock(DefaultAzureCredential.class));
                })) {

      AzureStorageClientProvider azureClientProviderSpy =
          Mockito.spy(new AzureStorageClientProvider(config));
      AzureStorageClientProvider.resetAzureAsyncClient();

      if (!isRefreshClient) {
        DataLakeServiceAsyncClient result = azureClientProviderSpy.getAzureAsyncClient();
        assertNotNull(result);
      } else {
        azureClientProviderSpy.refreshClient();
      }

      verify(azureClientProviderSpy, times(1)).createAzureAsyncClient();
    }
  }
}
