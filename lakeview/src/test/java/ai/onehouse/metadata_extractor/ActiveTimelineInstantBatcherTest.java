package ai.onehouse.metadata_extractor;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.storage.models.File;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ActiveTimelineInstantBatcherTest {

  private ActiveTimelineInstantBatcher activeTimelineInstantBatcher;

  @Mock private Config config;

  @Mock private MetadataExtractorConfig extractorConfig;

  @BeforeEach
  void setup(TestInfo testInfo) {
    if (testInfo.getDisplayName().equals("testWithInvalidBatchSize()")) {
      activeTimelineInstantBatcher = new ActiveTimelineInstantBatcher(config);
      return;
    }
    when(config.getMetadataExtractorConfig()).thenReturn(extractorConfig);
    if (testInfo.getTags().contains("NonBlocking")) {
      when(extractorConfig.getUploadStrategy())
          .thenReturn(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT);
    } else {
      when(extractorConfig.getUploadStrategy())
          .thenReturn(MetadataExtractorConfig.UploadStrategy.BLOCK_ON_INCOMPLETE_COMMIT);
    }

    activeTimelineInstantBatcher = new ActiveTimelineInstantBatcher(config);
  }

  @Test
  void testCreateBatchForTableWithNoCommit() {
    List<File> files = Collections.singletonList(generateFileObj("hoodie.properties"));

    List<List<File>> expectedBatches =
        Arrays.asList(Collections.singletonList(generateFileObj("hoodie.properties")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testIncompleteInitialCommit() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("hoodie.properties"));

    // instants with timestamp 222 need to be ignored as the actionType is unknown and
    // instants with timestamp 333 are skipped as the commit is not yet complete
    List<List<File>> expectedBatches =
        Arrays.asList(Collections.singletonList(generateFileObj("hoodie.properties")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testRollbackWithoutInflightAndRequestedInTheMiddle() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("222.rollback"),
            generateFileObj("111.inflight"),
            generateFileObj("333.clean"),
            generateFileObj("111.commit"),
            generateFileObj("444.action4.inflight"),
            generateFileObj("444.action4.requested"),
            generateFileObj("333.clean.requested"),
            generateFileObj("333.clean.inflight"),
            generateFileObj("hoodie.properties"));

    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")),
            Arrays.asList(
                generateFileObj("222.rollback"),
                generateFileObj("333.clean"),
                generateFileObj("333.clean.inflight"),
                generateFileObj("333.clean.requested")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testRollbackWithoutInflightAndRequestedAtEnd() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("555.rollback"),
            generateFileObj("111.inflight"),
            generateFileObj("333.clean"),
            generateFileObj("111.commit"),
            generateFileObj("333.clean.requested"),
            generateFileObj("333.clean.inflight"),
            generateFileObj("hoodie.properties"));

    List<List<File>> expectedBatches =
        Arrays.asList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")),
            Arrays.asList(
                generateFileObj("333.clean"),
                generateFileObj("333.clean.inflight"),
                generateFileObj("333.clean.requested")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testRollbackIncompleteAction1() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("111.inflight"),
            generateFileObj("111.commit"),
            generateFileObj("555.rollback.inflight"),
            generateFileObj("555.rollback.requested"),
            generateFileObj("hoodie.properties"));

    List<List<File>> expectedBatches =
        Collections.singletonList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testRollbackIncompleteAction2() {
    List<File> files =
        Arrays.asList(
            generateFileObj("111.commit.requested"),
            generateFileObj("111.inflight"),
            generateFileObj("111.commit"),
            generateFileObj("555.rollback.requested"),
            generateFileObj("hoodie.properties"));

    List<List<File>> expectedBatches =
        Collections.singletonList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getMiddle();
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
    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(instants, 4, getCheckpoint()).getMiddle();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test
  void testWithInvalidBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            activeTimelineInstantBatcher.createBatches(
                Collections.emptyList(), 2, getCheckpoint()));
  }

  @Tag("NonBlocking")
  @ParameterizedTest
  @MethodSource("createNonBlockingModeTestCases")
  void testNonBlockingMode(
      List<File> inputFiles,
      List<List<File>> expectedBatches,
      Checkpoint inputCheckpoint,
      String expectedFirstIncompleteCommit) {
    Triple<String, List<List<File>>, String> incompleteCommitBatchesPair =
        activeTimelineInstantBatcher.createBatches(inputFiles, 4, inputCheckpoint);
    assertEquals(expectedBatches, incompleteCommitBatchesPair.getMiddle());
    assertEquals(expectedFirstIncompleteCommit, incompleteCommitBatchesPair.getLeft());
  }

  static Stream<Arguments> createNonBlockingModeTestCases() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(
                generateFileObj("111.deltacommit.requested"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("hoodie.properties")),
            Arrays.asList(Collections.singletonList(generateFileObj("hoodie.properties"))),
            getCheckpoint(),
            null),
        Arguments.of(
            Arrays.asList(
                generateFileObj("111.deltacommit.requested"), // incomplete commit
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("333.clean"),
                generateFileObj("444.rollback.requested"), // incomplete in the end
                generateFileObj("333.clean.requested"),
                generateFileObj("222.unknown.inflight"), // invalid commit
                generateFileObj("333.clean.inflight"),
                generateFileObj("222.unknown.requested"),
                generateFileObj("444.rollback.inflight"),
                generateFileObj("222.unknown"),
                generateFileObj("hoodie.properties")),
            Arrays.asList(
                Arrays.asList(
                    generateFileObj("hoodie.properties"),
                    generateFileObj("333.clean"),
                    generateFileObj("333.clean.inflight"),
                    generateFileObj("333.clean.requested"))), // iteration is not blocked
            getCheckpoint(),
            "110" // next processing will start from the first missed incomplete commit
            ),
        Arguments.of(
            Arrays.asList(
                // 111 is incomplete commit
                generateFileObj("111.deltacommit.requested"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit"),
                generateFileObj("333.clean"),
                generateFileObj("444.rollback.requested"), // Incomplete commit
                generateFileObj("333.clean.requested"),
                generateFileObj(
                    "222.clean.inflight",
                    "21-07-2024"), // incomplete commit skipped but not completed in subsequent run
                generateFileObj("333.clean.inflight"),
                generateFileObj("222.clean.requested", "21-07-2024"),
                generateFileObj("444.rollback.inflight"),
                generateFileObj("666.rollback.requested"), // Incomplete commit
                generateFileObj("777.rollback.requested"),
                generateFileObj("777.rollback.inflight"),
                generateFileObj("777.rollback")),
            Arrays.asList(
                Arrays.asList(
                    generateFileObj("111.deltacommit"),
                    generateFileObj("111.deltacommit.inflight"),
                    generateFileObj("111.deltacommit.requested")),
                Arrays.asList(
                    generateFileObj("333.clean"),
                    generateFileObj("333.clean.inflight"),
                    generateFileObj("333.clean.requested")),
                Arrays.asList(
                    generateFileObj("777.rollback"),
                    generateFileObj("777.rollback.inflight"),
                    generateFileObj("777.rollback.requested"))),
            getCheckpoint().toBuilder().firstIncompleteCommitFile("500").build(),
            "443"));
  }

  static File generateFileObj(String fileName) {
    return generateFileObj(fileName, "23-07-2024");
  }

  static File generateFileObj(String fileName, String dateString) {
    Instant instant =
        LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
            .atStartOfDay()
            .toInstant(java.time.ZoneOffset.UTC);
    return File.builder().filename(fileName).isDirectory(false).lastModifiedAt(instant).build();
  }

  static Checkpoint getCheckpoint() {
    return getCheckpoint("22-07-2024");
  }

  static Checkpoint getCheckpoint(String dateString) {
    Instant instant =
        LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
            .atStartOfDay()
            .toInstant(java.time.ZoneOffset.UTC);
    return Checkpoint.builder()
        .checkpointTimestamp(instant)
        .batchId(0)
        .lastUploadedFile("12")
        .archivedCommitsProcessed(true)
        .build();
  }
}
