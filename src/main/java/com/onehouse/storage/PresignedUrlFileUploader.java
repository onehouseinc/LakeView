package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.constants.MetricsConstants;
import com.onehouse.metrics.LakeViewExtractorMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

@Slf4j
public class PresignedUrlFileUploader {
  private final AsyncStorageClient asyncStorageClient;
  private final AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;

  @Inject
  public PresignedUrlFileUploader(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull AsyncHttpClientWithRetry asyncHttpClientWithRetry,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.asyncHttpClientWithRetry = asyncHttpClientWithRetry;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(
      String presignedUrl, String fileUrl, int fileUploadStreamBatchSize) {
    log.debug("Uploading {} to retrieved presigned url", fileUrl);
    return asyncStorageClient
        .streamFileAsync(fileUrl)
        .thenCompose(
            fileStreamData ->
                CompletableFuture.runAsync(
                    () -> {
                      Request request =
                          new Request.Builder()
                              .url(presignedUrl)
                              .put(
                                  // okhttp streaming:
                                  // https://github.com/square/okhttp/blob/master/samples/guide/src/main/java/okhttp3/recipes/PostStreaming.java
                                  new RequestBody() {
                                    @Override
                                    public MediaType contentType() {
                                      return MediaType.parse("application/octet-stream");
                                    }

                                    @Override
                                    public void writeTo(BufferedSink sink) throws IOException {
                                      try (InputStream is = fileStreamData.getInputStream()) {
                                        byte[] buffer = new byte[fileUploadStreamBatchSize];
                                        int bytesRead;
                                        while ((bytesRead = is.read(buffer)) != -1) {
                                          sink.write(buffer, 0, bytesRead);
                                          sink.flush();
                                        }
                                      }
                                    }
                                  })
                              .build();

                      asyncHttpClientWithRetry
                          .makeRequestWithRetry(request)
                          .thenAccept(
                              response -> {
                                try (Response ignored = response) {
                                  if (!response.isSuccessful()) {
                                    int statusCode = response.code();
                                    String message = response.message();
                                    hudiMetadataExtractorMetrics
                                        .incrementTableMetadataProcessingFailureCounter(
                                            MetricsConstants.MetadataUploadFailureReasons
                                                .PRESIGNED_URL_UPLOAD_FAILURE);
                                    throw new RuntimeException(
                                        String.format(
                                            "File upload failed: response code: %s error message: %s",
                                            statusCode, message));
                                  }
                                }
                              })
                          .join(); // Wait for the upload to complete
                    }));
  }
}
