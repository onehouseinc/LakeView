package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static ai.onehouse.constants.MetadataExtractorConstants.ROLLBACK_ACTION;
import static ai.onehouse.constants.MetadataExtractorConstants.SAVEPOINT_ACTION;
import static ai.onehouse.constants.MetadataExtractorConstants.WHITELISTED_ACTION_TYPES;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.storage.models.File;
import com.google.cloud.Tuple;
import com.google.inject.Inject;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class ActiveTimelineInstantBatcher {
  private final MetadataExtractorConfig extractorConfig;

  @Inject
  public ActiveTimelineInstantBatcher(@Nonnull Config config) {
    this.extractorConfig = config.getMetadataExtractorConfig();
  }

  /**
   * Creates batches of Hudi instants, ensuring related instants are grouped together.
   *
   * @param instants The list of Hudi instants.
   * @param maxBatchSize the maximum number of instants per batch.
   * @return A list of batches, each batch being a list of instants.
   */
  public Triple<String, List<List<File>>, String> createBatches(
      List<File> instants, int maxBatchSize, Checkpoint checkpoint) {
    if (maxBatchSize < 3) {
      throw new IllegalArgumentException("max batch size cannot be less than 3");
    }

    List<File> sortedInstants;
    if (extractorConfig
        .getUploadStrategy()
        .equals(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT)) {
      // Get sorted instants by grouping them if they belong to the same commit and any of the files
      // has a last modified which is greater than the lastModified of the last checkpoint that was
      // uploaded
      sortedInstants = sortAndFilterInstants(instants, checkpoint.getCheckpointTimestamp());
    } else {
      sortedInstants = sortAndFilterInstants(instants);
    }

    List<List<File>> batches = new ArrayList<>();
    List<File> currentBatch = new ArrayList<>();
    String firstIncompleteCheckpoint = checkpoint.getFirstIncompleteCommitFile();
    String lastUnprocessedFile = "";

    int startIndex = 0;
    if (!sortedInstants.isEmpty()
        && sortedInstants.get(0).getFilename().equals(HOODIE_PROPERTIES_FILE)) {
      startIndex = 1;
      currentBatch.add(sortedInstants.get(0));
    }

    // Stop threshold is set to sortedInstants.size() - 2 to ensure we don't miss the case
    // when timeline ends with a completed savepoint action
    int index = startIndex;
    while (index <= sortedInstants.size() - 2) {
      ActiveTimelineInstant instant1 =
          getActiveTimeLineInstant(sortedInstants.get(index).getFilename());

      int groupSize = 3;
      boolean areInstantsInGrpRelated;
      boolean shouldStopIteration = false;
      if (instant1.action.equals(ROLLBACK_ACTION)) {
        // For rollback action, requested or inflight commits will be present unless there is
        // some error while restoring. Since rollback is not used when calculating metrics,
        // we don't want to be blocked by unusual rollback status.
        if (index + 2 >= sortedInstants.size()) {
          // If the latest rollback is not complete or there is a single completed rollback at the
          // end.
          // For the second case, we can upload in the following batch as rollback doesn't affect
          // metrics.
          areInstantsInGrpRelated = false;
          shouldStopIteration = true;
          lastUnprocessedFile = instant1.getTimestamp();
        } else {
          ActiveTimelineInstant instant2 =
              getActiveTimeLineInstant(sortedInstants.get(index + 1).getFilename());
          ActiveTimelineInstant instant3 =
              getActiveTimeLineInstant(sortedInstants.get(index + 2).getFilename());
          areInstantsInGrpRelated = areRelatedInstants(instant1, instant2, instant3);
          if (!areInstantsInGrpRelated && instant1.getState().equals("completed")) {
            groupSize = 1;
            areInstantsInGrpRelated = true;
          }
        }
      } else if (instant1.action.equals(SAVEPOINT_ACTION)) {
        if (index + 1 >= sortedInstants.size()) {
          // If the latest commit is not complete
          areInstantsInGrpRelated = false;
          shouldStopIteration = true;
          lastUnprocessedFile = instant1.getTimestamp();
        } else {
          ActiveTimelineInstant instant2 =
              getActiveTimeLineInstant(sortedInstants.get(index + 1).getFilename());
          areInstantsInGrpRelated = areRelatedSavepointInstants(instant1, instant2);
          groupSize = 2;
        }
      } else {
        if (index + 2 >= sortedInstants.size()) {
          // If the latest commit is not complete
          areInstantsInGrpRelated = false;
          shouldStopIteration = true;
          lastUnprocessedFile = instant1.getTimestamp();
        } else {
          ActiveTimelineInstant instant2 =
              getActiveTimeLineInstant(sortedInstants.get(index + 1).getFilename());
          ActiveTimelineInstant instant3 =
              getActiveTimeLineInstant(sortedInstants.get(index + 2).getFilename());
          areInstantsInGrpRelated = areRelatedInstants(instant1, instant2, instant3);
        }
      }

      if (areInstantsInGrpRelated) {
        if (currentBatch.size() + groupSize <= maxBatchSize) {
          // Add the next group of three instants to the current batch
          currentBatch.addAll(sortedInstants.subList(index, index + groupSize));
        } else {
          // Current batch size limit reached, start a new batch
          batches.add(new ArrayList<>(currentBatch));
          currentBatch.clear();
          currentBatch.addAll(sortedInstants.subList(index, index + groupSize));
        }
      } else if (!shouldStopIteration) {
        if (extractorConfig
            .getUploadStrategy()
            .equals(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT)) {
          // Instead of blocking the creation of batches, skipping the incomplete commit file and
          // updating the first incomplete checkpoint(startAfter) to be a unit before the incomplete
          // instant
          String firstIncompleteCheckpointUpdated =
              getFirstIncompleteCheckpoint(instant1.getTimestamp());
          if (StringUtils.isBlank(firstIncompleteCheckpoint)
              || firstIncompleteCheckpointUpdated.compareTo(firstIncompleteCheckpoint) < 0) {
            firstIncompleteCheckpoint = firstIncompleteCheckpointUpdated;
          }
          groupSize = 1;
        } else {
          shouldStopIteration = true;
        }
      }

      if (shouldStopIteration) {
        if (!currentBatch.isEmpty()) {
          batches.add(new ArrayList<>(currentBatch));
          currentBatch.clear();
        }
        break;
      }

      index += groupSize;
    }

    // Add any remaining instants in the current batch
    if (!currentBatch.isEmpty()) {
      batches.add(currentBatch);
    }


    return Triple.of(firstIncompleteCheckpoint, batches, getFirstIncompleteCheckpoint(lastUnprocessedFile));
  }

  public static String getFirstIncompleteCheckpoint(String numericString) {
    BigInteger number = new BigInteger(numericString);
    BigInteger decrementedNumber = number.subtract(BigInteger.ONE);
    return decrementedNumber.toString();
  }

  private List<File> sortAndFilterInstants(List<File> instants) {
    return instants.stream()
        .filter(this::filterFile)
        .sorted(getFileComparator())
        .collect(Collectors.toList());
  }

  private List<File> sortAndFilterInstants(List<File> instants, Instant lastModifiedFilter) {
    return instants.stream()
        .filter(this::filterFile)
        .collect(Collectors.groupingBy(file -> file.getFilename().split("\\.", 3)[0]))
        .values()
        .stream()
        .filter(
            group ->
                group.stream()
                    .anyMatch(
                        file ->
                            file.getFilename().equals(HOODIE_PROPERTIES_FILE)
                                || lastModifiedFilter.isBefore(file.getLastModifiedAt())))
        .flatMap(List::stream)
        .sorted(getFileComparator())
        .collect(Collectors.toList());
  }

  private boolean filterFile(File file) {
    return file.getFilename().equals(HOODIE_PROPERTIES_FILE)
        || WHITELISTED_ACTION_TYPES.contains(
            getActiveTimeLineInstant(file.getFilename()).getAction());
  }

  private Comparator<File> getFileComparator() {
    return Comparator.comparing(
        File::getFilename,
        (name1, name2) -> {
          if (HOODIE_PROPERTIES_FILE.equals(name1)) {
            return -1;
          } else if (HOODIE_PROPERTIES_FILE.equals(name2)) {
            return 1;
          }
          return name1.compareTo(name2);
        });
  }

  private static boolean areRelatedInstants(
      ActiveTimelineInstant instant1,
      ActiveTimelineInstant instant2,
      ActiveTimelineInstant instant3) {
    if (!instant1.getTimestamp().equals(instant2.getTimestamp())
        || !instant2.getTimestamp().equals(instant3.getTimestamp())) {
      return false;
    }

    // Check if all three states are present
    Set<String> states =
        new HashSet<>(Arrays.asList(instant1.getState(), instant2.getState(), instant3.getState()));
    return states.containsAll(Arrays.asList("inflight", "requested", "completed"));
  }

  // Savepoint instants only have inflight and final commit
  private static boolean areRelatedSavepointInstants(
      ActiveTimelineInstant instant1, ActiveTimelineInstant instant2) {
    if (!instant1.getTimestamp().equals(instant2.getTimestamp())) {
      return false;
    }

    Set<String> states = new HashSet<>(Arrays.asList(instant1.getState(), instant2.getState()));
    return states.containsAll(Arrays.asList("inflight", "completed"));
  }

  public static ActiveTimelineInstant getActiveTimeLineInstant(String instant) {
    String[] parts = instant.split("\\.", 3);

    String action;
    String state;
    // For commit action, metadata file in inflight state is in the format of XYZ.inflight
    if (parts.length == 2 && parts[1].equals("inflight")) {
      action = "commit";
      state = "inflight";
    } else {
      action = parts[1];
      state = parts.length == 3 ? parts[2] : "completed";
    }
    return ActiveTimelineInstant.builder().timestamp(parts[0]).action(action).state(state).build();
  }

  @Builder
  @Getter
  public static class ActiveTimelineInstant {
    private final String timestamp;
    private final String action;
    private final String state;
  }
}
