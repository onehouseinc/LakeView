package com.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onehouse.storage.models.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ActiveTimelineInstantBatcherTest {

  private ActiveTimelineInstantBatcher activeTimelineInstantBatcher;

  @BeforeEach
  void setup() {
    activeTimelineInstantBatcher = new ActiveTimelineInstantBatcher();
  }

  @Test
  void testCreateBatchWithExclusion() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("333.clean"),
            generateFileObj("111.deltacommit"),
            generateFileObj("444.rollback.requested"),
            generateFileObj("333.clean.requested"),
            generateFileObj("222.unknown.inflight"),
            generateFileObj("333.clean.inflight"),
            generateFileObj("222.unknown.requested"),
            generateFileObj("444.rollback.inflight"),
            generateFileObj("222.unknown"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 222 need to be ignored as the actionType is unknown and
    // instants with timestamp 333 are skipped as the commit is not yet complete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested")),
            Arrays.asList(
                generateFileObj("333.clean"),
                generateFileObj("333.clean.inflight"),
                generateFileObj("333.clean.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchWithCompactionCommits() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj(
                "222.commit"), // final instant of compaction commit will have action_type as commit
            generateFileObj("111.deltacommit"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("222.compaction.requested"),
            generateFileObj("222.compaction.inflight"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested")),
            Arrays.asList(
                generateFileObj("222.commit"),
                generateFileObj("222.compaction.inflight"),
                generateFileObj("222.compaction.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchWithSavepointCommits() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("222.savepoint"),
            generateFileObj("111.deltacommit"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("222.savepoint.inflight"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested")),
            Arrays.asList(
                generateFileObj("222.savepoint"), generateFileObj("222.savepoint.inflight")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchExcludeOne() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("222.clean"),
            generateFileObj("111.deltacommit"),
            generateFileObj("222.clean.requested"),
            generateFileObj("222.clean.inflight"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested")),
            Arrays.asList(
                generateFileObj("222.clean"),
                generateFileObj("222.clean.inflight"),
                generateFileObj("222.clean.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchMultiplePartialBatches() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("222.clean"),
            generateFileObj("111.deltacommit"),
            generateFileObj("444.action4.inflight"),
            generateFileObj("444.action4.requested"),
            generateFileObj("222.clean.requested"),
            generateFileObj("222.clean.inflight"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 and 444 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested")),
            Arrays.asList(
                generateFileObj("222.clean"),
                generateFileObj("222.clean.inflight"),
                generateFileObj("222.clean.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchWithCommitActions() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("111.inflight"),
            generateFileObj("222.clean"),
            generateFileObj("111.commit"),
            generateFileObj("444.action4.inflight"),
            generateFileObj("444.action4.requested"),
            generateFileObj("222.clean.requested"),
            generateFileObj("222.clean.inflight"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 and 444 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")),
            Arrays.asList(
                generateFileObj("222.clean"),
                generateFileObj("222.clean.inflight"),
                generateFileObj("222.clean.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchWithCommitAndSavepointAsMiddletBatchActions() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("555.rollback.requested"),
            generateFileObj("111.inflight"),
            generateFileObj("111.commit"),
            generateFileObj("555.rollback"),
            generateFileObj("444.savepoint.inflight"),
            generateFileObj("444.savepoint"),
            generateFileObj("555.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 and 444 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")),
            Arrays.asList(
                generateFileObj("444.savepoint"), generateFileObj("444.savepoint.inflight")),
            Arrays.asList(
                generateFileObj("555.rollback"),
                generateFileObj("555.rollback.inflight"),
                generateFileObj("555.rollback.requested")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testCreateBatchWithCommitAndSavepointAsLastBatchActions() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("111.inflight"),
            generateFileObj("111.commit"),
            generateFileObj("333.rollback"),
            generateFileObj("444.savepoint.inflight"),
            generateFileObj("444.savepoint"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 333 and 444 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")),
            Arrays.asList(
                generateFileObj("333.rollback"),
                generateFileObj("333.rollback.inflight"),
                generateFileObj("333.rollback.requested")),
            Arrays.asList(
                generateFileObj("444.savepoint"), generateFileObj("444.savepoint.inflight")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testIncompleteLastAction() {
    List<File> files =
        Arrays.asList(
            generateFileObj("555.commit.requested"),
            generateFileObj("333.rollback.requested"),
            generateFileObj("555.inflight"),
            generateFileObj("555.commit"),
            generateFileObj("666.replacecommit.inflight"),
            generateFileObj("444.savepoint"),
            generateFileObj("333.rollback"),
            generateFileObj("333.rollback.inflight"),
            generateFileObj("444.savepoint.inflight"),
            generateFileObj("666.replacecommit.requested"));

    // instants with timestamp 333 and 444 need to be ignored as the commit is incomplete
    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("333.rollback"),
                generateFileObj("333.rollback.inflight"),
                generateFileObj("333.rollback.requested")),
            Arrays.asList(
                generateFileObj("444.savepoint"), generateFileObj("444.savepoint.inflight")),
            Arrays.asList(
                generateFileObj("555.commit"),
                generateFileObj("555.commit.requested"),
                generateFileObj("555.inflight")));

    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(files, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  static Stream<Arguments> createBatchTestCases() {
    return Stream.of(
        // just hoodie.properties present
        Arguments.of(
            Collections.singletonList(generateFileObj("hoodie.properties")),
            Collections.singletonList(
                Collections.singletonList(generateFileObj("hoodie.properties")))),
        // no instants to batch
        Arguments.of(Collections.emptyList(), Collections.emptyList()),
        // incomplete instant
        Arguments.of(
            Collections.singletonList(generateFileObj("222.clean")), Collections.emptyList()));
  }

  @ParameterizedTest
  @MethodSource("createBatchTestCases")
  void testCreateBatchJustHoodieProperties(List<File> instants, List<List<File>> expectedBatches) {
    List<List<File>> actualBatches = activeTimelineInstantBatcher.createBatches(instants, 4);
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testWithInvalidBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> activeTimelineInstantBatcher.createBatches(Collections.emptyList(), 2));
  }

  private static File generateFileObj(String fileName) {
    return File.builder()
        .filename(fileName)
        .isDirectory(false)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }
}
