package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.WHITELISTED_ACTION_TYPES;

import com.onehouse.storage.models.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;

public class ActiveTimelineInstantBatcher {
  private static final String SAVEPOINT_ACTION = "savepoint";

  /**
   * Creates batches of Hudi instants, ensuring related instants are grouped together.
   *
   * @param instants The list of Hudi instants.
   * @param maxBatchSize the maximum number of instants per batch.
   * @return A list of batches, each batch being a list of instants.
   */
  public List<List<File>> createBatches(List<File> instants, int maxBatchSize) {
    if (maxBatchSize < 3) {
      throw new IllegalArgumentException("max batch size cannot be less than 3");
    }

    List<File> sortedInstants = sortAndFilterInstants(instants);
    List<List<File>> batches = new ArrayList<>();
    List<File> currentBatch = new ArrayList<>();

    int startIndex = 0;
    if (!sortedInstants.isEmpty()
        && sortedInstants.get(0).getFilename().equals(HOODIE_PROPERTIES_FILE)) {
      startIndex = 1;
      currentBatch.add(sortedInstants.get(0));
    }

    for (int index = startIndex; index <= sortedInstants.size() - 3; index += 3) {
      ActiveTimelineInstant instant1 =
          getActiveTimeLineInstant(sortedInstants.get(index).getFilename());
      ActiveTimelineInstant instant2 =
          getActiveTimeLineInstant(sortedInstants.get(index + 1).getFilename());

      int groupSize = 3;
      boolean areInstantsInGrpRelated;
      if (instant1.action.equals(SAVEPOINT_ACTION)) {
        areInstantsInGrpRelated = areRelatedSavepointInstants(instant1, instant2);
        groupSize = 2;
      } else {
        ActiveTimelineInstant instant3 =
            getActiveTimeLineInstant(sortedInstants.get(index + 2).getFilename());
        areInstantsInGrpRelated = areRelatedInstants(instant1, instant2, instant3);
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
      } else {
        // Instants are not related; add what we have and stop processing
        if (!currentBatch.isEmpty()) {
          batches.add(new ArrayList<>(currentBatch));
          currentBatch.clear();
        }
        break;
      }

      if (groupSize != 3) {
        index = index - 1;
      }
    }

    // Add any remaining instants in the current batch
    if (!currentBatch.isEmpty()) {
      batches.add(currentBatch);
    }

    return batches;
  }

  private List<File> sortAndFilterInstants(List<File> instants) {
    return instants.stream()
        .filter(
            file ->
                file.getFilename().equals(HOODIE_PROPERTIES_FILE)
                    || WHITELISTED_ACTION_TYPES.contains(
                        getActiveTimeLineInstant(file.getFilename()).action))
        .sorted(
            Comparator.comparing(
                File::getFilename,
                (name1, name2) -> {
                  if (HOODIE_PROPERTIES_FILE.equals(name1)) {
                    return -1;
                  } else if (HOODIE_PROPERTIES_FILE.equals(name2)) {
                    return 1;
                  }
                  return name1.compareTo(name2);
                }))
        .collect(Collectors.toList());
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

  private static ActiveTimelineInstant getActiveTimeLineInstant(String instant) {
    String[] parts = instant.split("\\.", 3);
    String state = parts.length == 3 ? parts[2] : "completed";
    return ActiveTimelineInstant.builder()
        .timestamp(parts[0])
        .action(parts[1])
        .state(state)
        .build();
  }

  @Builder
  @Getter
  private static class ActiveTimelineInstant {
    private final String timestamp;
    private final String action;
    private final String state;
  }
}
