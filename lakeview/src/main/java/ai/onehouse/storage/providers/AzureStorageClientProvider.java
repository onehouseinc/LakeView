package ai.onehouse.storage.providers;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.AzureConfig;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageClientProvider {
  private final AzureConfig azureConfig;
  private static BlobServiceAsyncClient azureAsyncClient;
  private static final Logger logger = LoggerFactory.getLogger(AzureStorageClientProvider.class);

  @Inject
  public AzureStorageClientProvider(@Nonnull Config config) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    this.azureConfig = fileSystemConfiguration.getAzureConfig();
  }

  @VisibleForTesting
  protected BlobServiceAsyncClient createAzureAsyncClient() {
    logger.debug("Instantiating Azure Blob Storage client");
    validateAzureConfig(azureConfig);

    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
    String endpoint = String.format("https://%s.blob.core.windows.net", azureConfig.getAccountName());
    builder.endpoint(endpoint);

    // Option 1: Connection String (includes account key and endpoint)
    Optional<String> connectionStringOpt = azureConfig.getConnectionString();
    if (connectionStringOpt.isPresent()) {
      logger.debug("Using connection string for authentication");
      builder.connectionString(connectionStringOpt.get());
      return builder.buildAsyncClient();
    }

    // Option 2: Account Key (shared key credential)
    Optional<String> accountKeyOpt = azureConfig.getAccountKey();
    if (accountKeyOpt.isPresent()) {
      logger.debug("Using account key for authentication");
      StorageSharedKeyCredential credential =
          new StorageSharedKeyCredential(azureConfig.getAccountName(), accountKeyOpt.get());
      builder.credential(credential);
      return builder.buildAsyncClient();
    }

    // Option 3: Service Principal (client secret credential)
    Optional<String> tenantIdOpt = azureConfig.getTenantId();
    Optional<String> clientIdOpt = azureConfig.getClientId();
    Optional<String> clientSecretOpt = azureConfig.getClientSecret();
    if (tenantIdOpt.isPresent() && clientIdOpt.isPresent() && clientSecretOpt.isPresent()) {
      logger.debug("Using service principal (client secret) for authentication");
      ClientSecretCredential credential =
          new ClientSecretCredentialBuilder()
              .tenantId(tenantIdOpt.get())
              .clientId(clientIdOpt.get())
              .clientSecret(clientSecretOpt.get())
              .build();
      builder.credential(credential);
      return builder.buildAsyncClient();
    }

    // Option 4: Default Azure Credential (fallback to environment-based auth)
    logger.debug("Using default Azure credential chain for authentication");
    DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();
    builder.credential(credential);
    return builder.buildAsyncClient();
  }

  public BlobServiceAsyncClient getAzureAsyncClient() {
    if (azureAsyncClient == null) {
      azureAsyncClient = createAzureAsyncClient();
    }
    return azureAsyncClient;
  }

  public void refreshClient() {
    azureAsyncClient = createAzureAsyncClient();
  }

  private void validateAzureConfig(AzureConfig azureConfig) {
    if (azureConfig == null) {
      throw new IllegalArgumentException("Azure Config not found");
    }

    if (StringUtils.isBlank(azureConfig.getAccountName())) {
      throw new IllegalArgumentException("Azure storage account name cannot be empty");
    }
  }

  @VisibleForTesting
  static void resetAzureAsyncClient() {
    azureAsyncClient = null;
  }
}
