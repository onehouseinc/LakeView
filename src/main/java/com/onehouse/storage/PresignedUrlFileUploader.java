package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.AsyncHttpClientWithRetry;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import com.onehouse.constants.MetricsConstants;
import com.onehouse.metrics.HudiMetadataExtractorMetrics;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Request;
import okhttp3.RequestBody;

@Slf4j
public class PresignedUrlFileUploader {
  private final AsyncStorageClient asyncStorageClient;
  private final AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private final HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics;

  @Inject
  public PresignedUrlFileUploader(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull AsyncHttpClientWithRetry asyncHttpClientWithRetry,
  @Nonnull HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.asyncHttpClientWithRetry = asyncHttpClientWithRetry;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
    log.debug("Uploading {} to retrieved presigned url", fileUrl);
    return asyncStorageClient
        .readFileAsBytes(fileUrl)
        .thenComposeAsync(
            response -> {
              RequestBody requestBody = RequestBody.create(response);
              Request request = new Request.Builder().url(presignedUrl).put(requestBody).build();

              return asyncHttpClientWithRetry
                  .makeRequestWithRetry(request)
                  .thenApply(
                      uploadResponse -> {
                        if (!uploadResponse.isSuccessful()) {
                          int statusCode = uploadResponse.code();
                          String message = uploadResponse.message();
                          uploadResponse.close();
                            hudiMetadataExtractorMetrics.incrementTableMetadataUploadFailureCounter(MetricsConstants.MetadataUploadFailureReasons.PRESIGNED_URL_UPLOAD_FAILURE);
                          throw new RuntimeException(
                              String.format(
                                  "file upload failed failed: response code: %s error message: %s",
                                  statusCode, message));
                        }
                        uploadResponse.close();
                        return null; // Successfully uploaded
                      });
            });
  }
}
