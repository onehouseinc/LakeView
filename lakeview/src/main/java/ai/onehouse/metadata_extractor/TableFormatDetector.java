package ai.onehouse.metadata_extractor;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.models.File;
import java.util.List;

/**
 * Identifies whether a directory listing represents the root of a table of a particular format.
 *
 * <p>One implementation per supported format (Hudi, Iceberg, ...). {@link TableDiscoveryService}
 * iterates registered detectors and stops at the first match, so detectors should be cheap and
 * unambiguous.
 */
public interface TableFormatDetector {
  /** Format this detector identifies. */
  TableFormat format();

  /** Returns true if the listed files at a directory indicate a table root of {@link #format()}. */
  boolean matches(List<File> listedFiles);
}
