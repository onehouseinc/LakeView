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
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(files, 4, getCheckpoint()).getRight();
    assertEquals(expectedBatches, actualBatches);
  }

  @Test void testRollBackWithJustInflight() {
    List<File> files = ImmutableList.of(generateFileObj("hoodie.properties"), generateFileObj("20230826085924.rollback"),
        generateFileObj("20230826085924.rollback.inflight"),
        generateFileObj("20230828084151.rollback"),
        generateFileObj("20230828084151.rollback.inflight"),
        generateFileObj("20230828090242.rollback"),
        generateFileObj("20230828090242.rollback.inflight"),
        generateFileObj("20230828090428.rollback"),
        generateFileObj("20230828090428.rollback.inflight"),
        generateFileObj("20240123165642.rollback"),
        generateFileObj("20240123165642.rollback.inflight"),
        generateFileObj("20240214150000.rollback"),
        generateFileObj("20240214150000.rollback.inflight"),
        generateFileObj("20240214151723.rollback"),
        generateFileObj("20240214151723.rollback.inflight"),
        generateFileObj("20240215143514.rollback"),
        generateFileObj("20240215143514.rollback.inflight"),
        generateFileObj("20240226181528.rollback"),
        generateFileObj("20240226181528.rollback.inflight"),
        generateFileObj("20240716154510.rollback"),
        generateFileObj("20240716154510.rollback.inflight"),
        generateFileObj("20240729000112.rollback"),
        generateFileObj("20240729000112.rollback.inflight"),
        generateFileObj("20240923095231.rollback"),
        generateFileObj("20240923095231.rollback.inflight"),
        generateFileObj("20240923095659.rollback"),
        generateFileObj("20240923095659.rollback.inflight"),
        generateFileObj("20240925152533.rollback"),
        generateFileObj("20240925152533.rollback.inflight"),
        generateFileObj("20241009152028.rollback"),
        generateFileObj("20241009152028.rollback.inflight"),
        generateFileObj("20241009152256.rollback"),
        generateFileObj("20241009152256.rollback.inflight"),
        generateFileObj("20241028152010.rollback"),
        generateFileObj("20241028152010.rollback.inflight"),
        generateFileObj("20241028154020.rollback"),
        generateFileObj("20241028154020.rollback.inflight"),
        generateFileObj("20241028154233.rollback"),
        generateFileObj("20241028154233.rollback.inflight"),
        generateFileObj("20241028154348.rollback"),
        generateFileObj("20241028154348.rollback.inflight"),
        generateFileObj("20250325191932427.commit"),
        generateFileObj("20250325191932427.commit.requested"),
        generateFileObj("20250325191932427.inflight"),
        generateFileObj("20250327185907537.commit"),
        generateFileObj("20250327185907537.commit.requested"),
        generateFileObj("20250327185907537.inflight"),
        generateFileObj("20250327192838460.commit"),
        generateFileObj("20250327192838460.commit.requested"),
        generateFileObj("20250327192838460.inflight"),
        generateFileObj("20250327193123670.clean"),
        generateFileObj("20250327193123670.clean.inflight"),
        generateFileObj("20250327193123670.clean.requested"),
        generateFileObj("20250329161732343.commit"),
        generateFileObj("20250329161732343.commit.requested"),
        generateFileObj("20250329161732343.inflight"),
        generateFileObj("20250329162654069.clean"),
        generateFileObj("20250329162654069.clean.inflight"),
        generateFileObj("20250329162654069.clean.requested"),
        generateFileObj("20250329162918512.commit"),
        generateFileObj("20250329162918512.commit.requested"),
        generateFileObj("20250329162918512.inflight"),
        generateFileObj("20250329163150721.clean"),
        generateFileObj("20250329163150721.clean.inflight"),
        generateFileObj("20250329163150721.clean.requested"),
        generateFileObj("20250330161832966.commit"),
        generateFileObj("20250330161832966.commit.requested"),
        generateFileObj("20250330161832966.inflight"),
        generateFileObj("20250330162816889.clean"),
        generateFileObj("20250330162816889.clean.inflight"),
        generateFileObj("20250330162816889.clean.requested"),
        generateFileObj("20250330163018354.commit"),
        generateFileObj("20250330163018354.commit.requested"),
        generateFileObj("20250330163018354.inflight"),
        generateFileObj("20250330163245281.clean"),
        generateFileObj("20250330163245281.clean.inflight"),
        generateFileObj("20250330163245281.clean.requested"),
        generateFileObj("20250405161835919.commit"),
        generateFileObj("20250405161835919.commit.requested"),
        generateFileObj("20250405161835919.inflight"),
        generateFileObj("20250405162650015.clean"),
        generateFileObj("20250405162650015.clean.inflight"),
        generateFileObj("20250405162650015.clean.requested"),
        generateFileObj("20250405162917353.commit"),
        generateFileObj("20250405162917353.commit.requested"),
        generateFileObj("20250405162917353.inflight"),
        generateFileObj("20250405163133088.clean"),
        generateFileObj("20250405163133088.clean.inflight"),
        generateFileObj("20250405163133088.clean.requested"),
        generateFileObj("20250406161840587.commit"),
        generateFileObj("20250406161840587.commit.requested"),
        generateFileObj("20250406161840587.inflight"),
        generateFileObj("20250406162748896.clean"),
        generateFileObj("20250406162748896.clean.inflight"),
        generateFileObj("20250406162748896.clean.requested"),
        generateFileObj("20250406162946078.commit"),
        generateFileObj("20250406162946078.commit.requested"),
        generateFileObj("20250406162946078.inflight"),
        generateFileObj("20250406163158665.clean"),
        generateFileObj("20250406163158665.clean.inflight"),
        generateFileObj("20250406163158665.clean.requested"),
        generateFileObj("20250412161804168.commit"),
        generateFileObj("20250412161804168.commit.requested"),
        generateFileObj("20250412161804168.inflight"),
        generateFileObj("20250412162459605.clean"),
        generateFileObj("20250412162459605.clean.inflight"),
        generateFileObj("20250412162459605.clean.requested"),
        generateFileObj("20250412162714025.commit"),
        generateFileObj("20250412162714025.commit.requested"),
        generateFileObj("20250412162714025.inflight"),
        generateFileObj("20250412162927587.clean"),
        generateFileObj("20250412162927587.clean.inflight"),
        generateFileObj("20250412162927587.clean.requested"),
        generateFileObj("20250413161711962.commit"),
        generateFileObj("20250413161711962.commit.requested"),
        generateFileObj("20250413161711962.inflight"),
        generateFileObj("20250413162350331.clean"),
        generateFileObj("20250413162350331.clean.inflight"),
        generateFileObj("20250413162350331.clean.requested"),
        generateFileObj("20250413162604419.commit"),
        generateFileObj("20250413162604419.commit.requested"),
        generateFileObj("20250413162604419.inflight"),
        generateFileObj("20250413162818634.clean"),
        generateFileObj("20250413162818634.clean.inflight"),
        generateFileObj("20250413162818634.clean.requested"),
        generateFileObj("20250419121937721.commit"),
        generateFileObj("20250419121937721.commit.requested"),
        generateFileObj("20250419121937721.inflight"),
        generateFileObj("20250419122804363.clean"),
        generateFileObj("20250419122804363.clean.inflight"),
        generateFileObj("20250419122804363.clean.requested"),
        generateFileObj("20250419123030632.commit"),
        generateFileObj("20250419123030632.commit.requested"),
        generateFileObj("20250419123030632.inflight"),
        generateFileObj("20250419123444863.clean"),
        generateFileObj("20250419123444863.clean.inflight"),
        generateFileObj("20250419123444863.clean.requested"),
        generateFileObj("20250419161754834.commit"),
        generateFileObj("20250419161754834.commit.requested"),
        generateFileObj("20250419161754834.inflight"),
        generateFileObj("20250419162659553.clean"),
        generateFileObj("20250419162659553.clean.inflight"),
        generateFileObj("20250419162659553.clean.requested"),
        generateFileObj("20250419162909328.commit"),
        generateFileObj("20250419162909328.commit.requested"),
        generateFileObj("20250419162909328.inflight"),
        generateFileObj("20250419163129505.clean"),
        generateFileObj("20250419163129505.clean.inflight"),
        generateFileObj("20250419163129505.clean.requested"),
        generateFileObj("20250420121401556.commit"),
        generateFileObj("20250420121401556.commit.requested"),
        generateFileObj("20250420121401556.inflight"),
        generateFileObj("20250420122149782.clean"),
        generateFileObj("20250420122149782.clean.inflight"),
        generateFileObj("20250420122149782.clean.requested"),
        generateFileObj("20250420122357088.commit"),
        generateFileObj("20250420122357088.commit.requested"),
        generateFileObj("20250420122357088.inflight"),
        generateFileObj("20250420122612096.clean"),
        generateFileObj("20250420122612096.clean.inflight"),
        generateFileObj("20250420122612096.clean.requested"),
        generateFileObj("20250420161820360.commit"),
        generateFileObj("20250420161820360.commit.requested"),
        generateFileObj("20250420161820360.inflight"),
        generateFileObj("20250420162605446.clean"),
        generateFileObj("20250420162605446.clean.inflight"),
        generateFileObj("20250420162605446.clean.requested"),
        generateFileObj("20250420162822423.commit"),
        generateFileObj("20250420162822423.commit.requested"),
        generateFileObj("20250420162822423.inflight"),
        generateFileObj("20250420163039555.clean"),
        generateFileObj("20250420163039555.clean.inflight"),
        generateFileObj("20250420163039555.clean.requested"),
        generateFileObj("20250421122048305.commit"),
        generateFileObj("20250421122048305.commit.requested"),
        generateFileObj("20250421122048305.inflight"),
        generateFileObj("20250421122852023.clean"),
        generateFileObj("20250421122852023.clean.inflight"),
        generateFileObj("20250421122852023.clean.requested"),
        generateFileObj("20250421123048650.commit"),
        generateFileObj("20250421123048650.commit.requested"),
        generateFileObj("20250421123048650.inflight"),
        generateFileObj("20250421123404591.clean"),
        generateFileObj("20250421123404591.clean.inflight"),
        generateFileObj("20250421123404591.clean.requested"),
        generateFileObj("20250422151200589.commit"),
        generateFileObj("20250422151200589.commit.requested"),
        generateFileObj("20250422151200589.inflight"),
        generateFileObj("20250422151306006.clean"),
        generateFileObj("20250422151306006.clean.inflight"),
        generateFileObj("20250422151306006.clean.requested"));

    List<List<File>> expectedBatches =
        Collections.singletonList(
            Arrays.asList(
                generateFileObj("hoodie.properties"),
                generateFileObj("111.commit"),
                generateFileObj("111.commit.requested"),
                generateFileObj("111.inflight")));

    List<List<File>> actualBatches =
        activeTimelineInstantBatcher.createBatches(files, 20, getCheckpoint()).getRight();
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
        activeTimelineInstantBatcher.createBatches(instants, 4, getCheckpoint()).getRight();
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
    Pair<String, List<List<File>>> incompleteCommitBatchesPair =
        activeTimelineInstantBatcher.createBatches(inputFiles, 4, inputCheckpoint);
    assertEquals(expectedBatches, incompleteCommitBatchesPair.getRight());
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
