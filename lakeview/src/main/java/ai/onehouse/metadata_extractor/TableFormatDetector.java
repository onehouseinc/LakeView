package ai.onehouse.metadata_extractor;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.models.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Identifies whether a directory listing represents the root of a table of a particular format.
 *
 * <p>One implementation per supported format (Hudi, Iceberg, ...). Each {@code Database} in the
 * parser YAML declares its {@code tableFormat}, and {@link TableDiscoveryService} picks the
 * single matching detector for that database — detectors are not raced against each other, so
 * there is no ordering trap.
 *
 * <p>{@link #matches} returns a {@link CompletableFuture} because some detectors need a follow-up
 * directory listing to confirm a match (e.g. Iceberg's "must have a {@code *.metadata.json}
 * inside {@code metadata/}" rule). Stateless detectors should return {@link
 * CompletableFuture#completedFuture}.
 */
public interface TableFormatDetector {
  /** Format this detector identifies. */
  TableFormat format();

  /**
   * Returns true if the directory at {@code path} (with its top-level listing already in {@code
   * listedFiles}) is a table root of {@link #format()}. Implementations may issue additional
   * storage I/O to validate.
   */
  CompletableFuture<Boolean> matches(String path, List<File> listedFiles);
}
