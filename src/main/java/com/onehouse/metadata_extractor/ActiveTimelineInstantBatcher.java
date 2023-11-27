package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;

import com.onehouse.storage.models.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;

public class ActiveTimelineInstantBatcher {

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

    List<File> sortedInstants =
        instants.stream()
            .sorted(
                Comparator.comparing(
                    File::getFilename,
                    (name1, name2) -> {
                      // hoodie.properties should come first
                      if (HOODIE_PROPERTIES_FILE.equals(name1)) {
                        return -1;
                      } else if (HOODIE_PROPERTIES_FILE.equals(name2)) {
                        return 1;
                      }
                      return name1.compareTo(name2); // Lexicographic sorting for other files
                    }))
            .collect(Collectors.toList());
    List<List<File>> batches = new ArrayList<>();
    int index = 0;

    while (index < sortedInstants.size()) {
      // Check if the remaining files are fewer than 3
      if (sortedInstants.size() - index < 3) {
        break; // Skip these files completely
      }

      int tentativeEndIndex = Math.min(index + maxBatchSize, sortedInstants.size());
      int actualEndIndex = adjustBatchEndIndex(sortedInstants, index, tentativeEndIndex);

      List<File> batch = new ArrayList<>(sortedInstants.subList(index, actualEndIndex));
      batches.add(batch);

      index = actualEndIndex; // Update index for the next batch
    }

    return batches;
  }

  /**
   * Adjusts the end index of the current batch to ensure that related instants are not split across
   * batches.
   *
   * @param instants The sorted list of Hudi instants.
   * @param startIndex The start index of the current batch.
   * @param tentativeEndIndex The tentative end index of the current batch.
   * @return The adjusted end index of the batch.
   */
  private static int adjustBatchEndIndex(
      List<File> instants, int startIndex, int tentativeEndIndex) {
    if (tentativeEndIndex - startIndex < 3) {
      return startIndex; // Not enough instants for a complete commit
    }

    ActiveTimelineInstant instant1 =
        getActiveTimeLineInstant(instants.get(tentativeEndIndex - 3).getFilename());
    ActiveTimelineInstant instant2 =
        getActiveTimeLineInstant(instants.get(tentativeEndIndex - 2).getFilename());
    ActiveTimelineInstant instant3 =
        getActiveTimeLineInstant(instants.get(tentativeEndIndex - 1).getFilename());

    if (areRelatedInstants(instant1, instant2, instant3)) {
      return tentativeEndIndex;
    } else {
      // Determine whether to exclude one or two instants from the batch
      if (instant1.getTimestamp().equals(instant2.getTimestamp())
          && instant1.getAction().equals(instant2.getAction())) {
        return tentativeEndIndex - 1;
      } else {
        return tentativeEndIndex - 2;
      }
    }
  }

  private static boolean areRelatedInstants(
      ActiveTimelineInstant instant1,
      ActiveTimelineInstant instant2,
      ActiveTimelineInstant instant3) {
    if (!instant1.getTimestamp().equals(instant2.getTimestamp())
        || !instant2.getTimestamp().equals(instant3.getTimestamp())) {
      return false;
    }

    if (!instant1.getAction().equals(instant2.getAction())
        || !instant2.getAction().equals(instant3.getAction())) {
      return false;
    }

    // Check if all three states are present
    Set<String> states =
        new HashSet<>(Arrays.asList(instant1.getState(), instant2.getState(), instant3.getState()));
    return states.containsAll(Arrays.asList("inflight", "requested", "completed"));
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
  @Value
  private static class ActiveTimelineInstant {
    String timestamp;
    String action;
    String state;
  }
}
