package ai.onehouse.metadata_extractor;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.storage.models.File;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
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

  Instant convert(String t) {
    //String dateTimeString = "2024-10-14 07:39:50";

    // Define the formatter matching the input string format
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Parse the string to LocalDateTime
    LocalDateTime localDateTime = LocalDateTime.parse(t, formatter);

    // Convert to Instant using UTC (ZoneOffset.UTC)
    Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
    return instant;
  }

  @Tag("NonBlocking")
  @Test
  void test() {
    String filePath = "/Users/karanmittal/Documents/Repos/LakeView/lakeview/src/test/resources/debug/cndr.txt";
    List<String> entries = new ArrayList<>();
    List<File> files = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        // Split the line by spaces
        String[] parts = line.trim().split("\\s+");
        if (parts.length >= 3) {
          // Extract the date (first two parts) and the file name (last part)
          String date = parts[0] + " " + parts[1];
          String fileName = parts[3];
          System.out.println("Date: " + date + ", File Name: " + fileName);
          entries.add("Date: " + date + ", File Name: " + fileName);
          File f = File.builder().filename(fileName).isDirectory(false).lastModifiedAt(convert(date)).build();
          files.add(f);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    //Checkpoint cp = getCP2();
    List<File> truncatedFIles = files.subList(0, files.size());
    Pair<String, List<List<File>>> actualBatches =
        activeTimelineInstantBatcher.createBatches(truncatedFIles, 4, getCP2());
    List<List<File>> batches = actualBatches.getRight();
    String cp = actualBatches.getLeft();
    System.out.println(batches);
    // 0 ... 1000 -> firstIncomplete = 0th
    // 1001 ... 2000
    // 2001 ... 3000
    System.out.println(cp);
  }


  @Tag("NonBlocking")
  @Test
  void test2() {
    String fName = "apna2";

    String filePath = String.format("/Users/karanmittal/Documents/Repos/LakeView/lakeview/src/test/resources/debug/%s.txt", fName);  // Replace with your file path
    List<File> files = new ArrayList<>();
    int count = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      String filePathLine = null;
      String creationTime = null;
      String updateTime = null;

      // Patterns for matching lines
      Pattern filePathPattern = Pattern.compile("^gs://prod-apna-data/(.+):$");

      Pattern creationTimePattern = Pattern.compile("Creation Time:\\s+(\\S+)");
      Pattern updateTimePattern = Pattern.compile("Update Time:\\s+(\\S+)");

      if (fName.equals("apna2")) {
        creationTimePattern = Pattern.compile("Creation time:\\s*(.*)");
        updateTimePattern = Pattern.compile("Update time:\\s*(.*)");
      }


      while ((line = reader.readLine()) != null) {
        line = line.trim();
        // Check for file path line
        Matcher filePathMatcher = filePathPattern.matcher(line);
        if (filePathMatcher.find()) {
          count++;
          filePathLine = "data/" + filePathMatcher.group(1); // Format the path as requested
        }

        // Check for creation time line
        Matcher creationMatcher = creationTimePattern.matcher(line);
        if (creationMatcher.find()) {
          creationTime = creationMatcher.group(1);
        }

        // Check for first update time line
        Matcher updateMatcher = updateTimePattern.matcher(line);
        if (updateMatcher.find() && updateTime == null) {  // Capture only the first Update Time
          updateTime = updateMatcher.group(1);
        }

//        if(fName.equals("apna2")) {
//          if (line.startsWith("Creation time:")) {
//            creationTime = line.split(":")[1].trim();
//          }
//
//          if (line.startsWith("Update time:")) {
//            updateTime = line.split(":")[1].trim();
//          }
//        }


        // Print and reset data at the end of each block (empty line signals end of current block)
        if (filePathLine != null && creationTime != null && updateTime != null) {
          String fileName = filePathLine.substring(filePathLine.lastIndexOf('/') + 1);
          System.out.println(filePathLine.substring(filePathLine.lastIndexOf('/') + 1) + " | Creation Time: " + creationTime + " | Update Time: " + updateTime);
          File f = File.builder().filename(fileName).isDirectory(false).lastModifiedAt(parseToInstant(updateTime)).build();
          files.add(f);
          filePathLine = null;
          creationTime = null;
          updateTime = null;

        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    List<File> truncatedFIles = files.subList(0, files.size());
    Pair<String, List<List<File>>> actualBatches =
        activeTimelineInstantBatcher.createBatches(truncatedFIles, 4, getCP3());
    List<List<File>> batches = actualBatches.getRight();
    String cp = actualBatches.getLeft();
    System.out.println(batches);
    // 0 ... 1000 -> firstIncomplete = 0th
    // 1001 ... 2000
    // 2001 ... 3000
    System.out.println(cp);


    // xyx
    // xyz.requested
    // xyz.inflight


    //Checkpoint cp = getCP2();
//    List<File> truncatedFIles = files.subList(0, files.size());
//    Pair<String, List<List<File>>> actualBatches =
//        activeTimelineInstantBatcher.createBatches(truncatedFIles, 4, getCP2());
//    List<List<File>> batches = actualBatches.getRight();
//    String cp = actualBatches.getLeft();
//    System.out.println(batches);
//    // 0 ... 1000 -> firstIncomplete = 0th
//    // 1001 ... 2000
//    // 2001 ... 3000
//    System.out.println(cp);
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

  static Checkpoint getCP2() {
    Instant instant = Instant.ofEpochSecond(1728954815);
    return Checkpoint.builder()
        .checkpointTimestamp(instant)
        .batchId(0)
        .lastUploadedFile("20241015011323784.deltacommit")
        .firstIncompleteCommitFile("")
        .archivedCommitsProcessed(true)
        .build();
  }
  // 20241019044830704

  static Checkpoint getCP3() {
    Instant instant = Instant.EPOCH;
    return Checkpoint.builder()
        .checkpointTimestamp(instant)
        .batchId(0)
        .lastUploadedFile("2021015011323784.deltacommit")
        .firstIncompleteCommitFile("")
        .archivedCommitsProcessed(true)
        .build();
  }

  private static Instant parseToInstant(String dateTime) {
    // Define the formatter for parsing the date string
    DateTimeFormatter formatter =  DateTimeFormatter
        .ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
    // Parse and convert to Instant
    return Instant.from(formatter.parse(dateTime));
  }
}
