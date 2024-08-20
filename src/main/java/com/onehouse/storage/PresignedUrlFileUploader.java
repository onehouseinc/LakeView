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

  //  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
  //    log.debug("Uploading {} to retrieved presigned url", fileUrl);
  //    return asyncStorageClient
  //        .readFileAsBytes(fileUrl)
  //        .thenComposeAsync(
  //            response -> {
  //              RequestBody requestBody = RequestBody.create(response);
  //              Request request = new
  // Request.Builder().url(presignedUrl).put(requestBody).build();
  //
  //              return asyncHttpClientWithRetry
  //                  .makeRequestWithRetry(request)
  //                  .thenApply(
  //                      uploadResponse -> {
  //                        if (!uploadResponse.isSuccessful()) {
  //                          int statusCode = uploadResponse.code();
  //                          String message = uploadResponse.message();
  //                          uploadResponse.close();
  //                          hudiMetadataExtractorMetrics
  //                              .incrementTableMetadataProcessingFailureCounter(
  //                                  MetricsConstants.MetadataUploadFailureReasons
  //                                      .PRESIGNED_URL_UPLOAD_FAILURE);
  //                          throw new RuntimeException(
  //                              String.format(
  //                                  "file upload failed failed: response code: %s error message:
  // %s",
  //                                  statusCode, message));
  //                        }
  //                        uploadResponse.close();
  //                        return null; // Successfully uploaded
  //                      });
  //            });
  //  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
    log.info("trying to upload to presigned url");
    return asyncStorageClient
        .readFileAsInputStream(fileUrl)
        .thenCompose(
            inputStream ->
                CompletableFuture.runAsync(
                    () -> {
                      log.info("read object as input stream");
                      Request request =
                          new Request.Builder()
                              .url(presignedUrl)
                              .put(
                                  new RequestBody() {
                                    @Override
                                    public MediaType contentType() {
                                      return MediaType.parse("application/octet-stream");
                                    }

                                    @Override
                                    public void writeTo(BufferedSink sink) throws IOException {
                                      try (InputStream is = inputStream) {
                                        byte[] buffer = new byte[5 * 1024 * 1024]; // 5MB buffer
                                        int bytesRead;
                                        while ((bytesRead = is.read(buffer)) != -1) {
                                          sink.write(buffer, 0, bytesRead);
                                        }
                                      }
                                    }
                                  })
                              .build();

                      asyncHttpClientWithRetry
                          .makeRequestWithRetry(request)
                          .thenAccept(
                              response -> {
                                try {
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
                                  log.info("Successfully uploaded to presigned URL");
                                } finally {
                                  response.close();
                                }
                              })
                          .join(); // Wait for the upload to complete
                    }));
  }
}
