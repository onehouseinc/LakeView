package com.onehouse.metadata_extractor;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThrows;

import com.onehouse.storage.models.File;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActiveTimelineInstantBatcherTest {

  private ActiveTimelineInstantBatcher activeTimelineInstantBatcher;

  @BeforeEach
  void setup() {
    activeTimelineInstantBatcher = new ActiveTimelineInstantBatcher();
  }

  @Test
  void testCreateBatchExcludeTwo() {
    List<File> files =
        List.of(
            generateFileObj("111.action1.requested"),
            generateFileObj("111.action1.inflight"),
            generateFileObj("222.action2"),
            generateFileObj("111.action1"),
            generateFileObj("333.action3.requested"),
            generateFileObj("222.action2.requested"),
            generateFileObj("222.action2.inflight"),
            generateFileObj("333.action3.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        List.of(
            List.of(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.action1"),
                generateFileObj("111.action1.inflight"),
                generateFileObj("111.action1.requested")),
            List.of(
                generateFileObj("222.action2"),
                generateFileObj("222.action2.inflight"),
                generateFileObj("222.action2.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchExcludeOne() {
    List<File> files =
        List.of(
            generateFileObj("111.action1.requested"),
            generateFileObj("111.action1.inflight"),
            generateFileObj("222.action2"),
            generateFileObj("111.action1"),
            generateFileObj("222.action2.requested"),
            generateFileObj("222.action2.inflight"),
            generateFileObj("333.action3.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        List.of(
            List.of(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.action1"),
                generateFileObj("111.action1.inflight"),
                generateFileObj("111.action1.requested")),
            List.of(
                generateFileObj("222.action2"),
                generateFileObj("222.action2.inflight"),
                generateFileObj("222.action2.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testWithInvalidBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          activeTimelineInstantBatcher.createBatches(List.of(), 2);
        });
  }

  private File generateFileObj(String fileName) {
    return File.builder()
        .filename(fileName)
        .isDirectory(false)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }
}
