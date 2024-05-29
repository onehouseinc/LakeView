package com.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.onehouse.api.models.request.TableType;
import com.onehouse.constants.MetricsConstants;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.metrics.HudiMetadataExtractorMetrics;
import com.onehouse.storage.AsyncStorageClient;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HoodiePropertiesReaderTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  @Mock private HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics;
  @InjectMocks private HoodiePropertiesReader hoodiePropertiesReader;

  @ParameterizedTest
  @EnumSource(TableType.class)
  void testReadHoodieProperties(TableType tableType)
      throws ExecutionException, InterruptedException {
    String path = "some/path/to/properties/file";
    String propertiesContent =
        String.format("hoodie.table.name=test_table\nhoodie.table.type=%s", tableType.toString());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(propertiesContent.getBytes());

    when(asyncStorageClient.readFileAsInputStream(path))
        .thenReturn(CompletableFuture.completedFuture(inputStream));

    CompletableFuture<ParsedHudiProperties> futureResult =
        hoodiePropertiesReader.readHoodieProperties(path);

    ParsedHudiProperties result = futureResult.get();
    assertEquals("test_table", result.getTableName());
    assertEquals(tableType, result.getTableType());
  }

  @Test
  void testReadHoodiePropertiesWithoutRequiredKeys() {
    String path = "some/path/to/properties/file";
    // wrong table name key, will throw error as required field is null
    String propertiesContent =
        "hoodie.table.identifier=test_table\nhoodie.table.type=COPY_ON_WRITE";
    ByteArrayInputStream inputStream = new ByteArrayInputStream(propertiesContent.getBytes());

    when(asyncStorageClient.readFileAsInputStream(path))
        .thenReturn(CompletableFuture.completedFuture(inputStream));

    CompletableFuture<ParsedHudiProperties> futureResult =
        hoodiePropertiesReader.readHoodieProperties(path);

    // on encountering error, readHoodieProperties returns null
    assertNull(futureResult.join());
  }

  @Test
  void testReadHoodiePropertiesEncountersError() {
    String path = "some/path/to/properties/file";

    when(asyncStorageClient.readFileAsInputStream(path))
        .thenReturn(failedFuture(new Exception("File not found")));
    CompletableFuture<ParsedHudiProperties> futureResult =
        hoodiePropertiesReader.readHoodieProperties(path);

    assertNull(futureResult.join());
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            MetricsConstants.MetadataUploadFailureReasons.HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED);
  }

  public static <R> CompletableFuture<R> failedFuture(Throwable error) {
    CompletableFuture<R> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
