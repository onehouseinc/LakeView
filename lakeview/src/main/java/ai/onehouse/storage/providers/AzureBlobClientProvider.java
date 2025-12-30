package ai.onehouse.storage.providers;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.AzureConfig;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class AzureBlobClientProvider {
  private final AzureConfig azureConfig;
  private static BlobServiceClient blobServiceClient;
  private static final Logger logger = LoggerFactory.getLogger(AzureBlobClientProvider.class);

  @Inject
  public AzureBlobClientProvider(@Nonnull Config config) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    this.azureConfig =
        fileSystemConfiguration.getAzureConfig() != null
            ? fileSystemConfiguration.getAzureConfig()
            : AzureConfig.builder().build();
  }

  @VisibleForTesting
  BlobServiceClient createBlobServiceClient() {
    logger.debug("Instantiating Azure Blob storage client");
    validateAzureConfig(azureConfig);

    String storageAccountName = azureConfig.getStorageAccountName()
        .orElseThrow(() -> new IllegalArgumentException("Azure storage account name is required"));

    // Build the endpoint URL
    String endpoint = String.format("https://%s.blob.core.windows.net", storageAccountName);

    BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
        .endpoint(endpoint);

    // Use DefaultAzureCredential which supports:
    // 1. Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    // 2. Managed Identity (when running on Azure)
    // 3. Azure CLI credentials
    TokenCredential credential = new DefaultAzureCredentialBuilder().build();
    builder.credential(credential);

    return builder.buildClient();
  }

  public BlobServiceClient getBlobServiceClient() {
    if (blobServiceClient == null) {
      blobServiceClient = createBlobServiceClient();
    }
    return blobServiceClient;
  }

  public BlobContainerClient getBlobContainerClient(String containerName) {
    return getBlobServiceClient().getBlobContainerClient(containerName);
  }

  public void refreshClient() {
    blobServiceClient = createBlobServiceClient();
  }

  private void validateAzureConfig(AzureConfig azureConfig) {
    if (azureConfig == null) {
      throw new IllegalArgumentException("Azure Config not found");
    }

    if (!azureConfig.getStorageAccountName().isPresent()
        || StringUtils.isBlank(azureConfig.getStorageAccountName().get())) {
      throw new IllegalArgumentException("Azure storage account name cannot be empty");
    }
  }
}



