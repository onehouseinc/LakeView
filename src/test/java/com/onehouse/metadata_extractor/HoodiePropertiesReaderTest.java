package com.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.onehouse.api.request.TableType;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.storage.AsyncStorageClient;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HoodiePropertiesReaderTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  @InjectMocks private HoodiePropertiesReader hoodiePropertiesReader;

  @Test
  void testReadHoodieProperties() throws ExecutionException, InterruptedException {
    String path = "some/path/to/properties/file";
    String propertiesContent = "hoodie.table.name=test_table\nhoodie.table.type=COPY_ON_WRITE";
    ByteArrayInputStream inputStream = new ByteArrayInputStream(propertiesContent.getBytes());

    when(asyncStorageClient.readFileAsInputStream(path))
        .thenReturn(CompletableFuture.completedFuture(inputStream));

    CompletableFuture<ParsedHudiProperties> futureResult =
        hoodiePropertiesReader.readHoodieProperties(path);

    ParsedHudiProperties result = futureResult.get();
    assertEquals("test_table", result.getTableName());
    assertEquals(TableType.COPY_ON_WRITE, result.getTableType());
  }
}
