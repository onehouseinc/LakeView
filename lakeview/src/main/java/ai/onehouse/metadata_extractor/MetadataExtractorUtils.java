package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.RateLimitException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public final class MetadataExtractorUtils {

    /**
     * Markers identifying the control plane's "this table is deleted, skip it" advisory, returned
     * as a per-table {@code error} string on an otherwise successful (HTTP 200)
     * initialize-tables response.
     *
     * <p>Matched on message text because
     * {@code InitializeTableMetricsCheckpointResponse.InitializeSingleTableMetricsCheckpointResponse}
     * carries only a free-text {@code error} field — there is no structured error/status code to
     * switch on. Replace this with a code check as soon as the API exposes one; see
     * {@link #isTableDeletedError(String)}.
     */
    private static final List<String> TABLE_DELETED_ERROR_MARKERS =
        Collections.unmodifiableList(
            Arrays.asList("skipped as it is deleted", "table is deleted"));

    private MetadataExtractorUtils(){}

    /**
     * Whether a per-table {@code error} from the initialize-tables response is the control plane's
     * benign "table has been deleted" advisory rather than an actual failure.
     *
     * <p>The control plane returns this on a <b>successful</b> response once a table has been
     * deleted control-plane side. It means "there is nothing to upload for this table", not "the
     * call failed" — so callers should skip the table quietly instead of counting an upload
     * failure. Treating it as a failure produces one ERROR log and one failure-counter increment
     * per deleted table per cycle, which on a large lake buries real problems (ENG-45879).
     *
     * @param error the per-table {@code error} string; may be {@code null} or blank
     * @return {@code true} if the error denotes a deleted table
     */
    public static boolean isTableDeletedError(String error) {
        if (error == null) {
            return false;
        }
        String normalized = error.toLowerCase(Locale.ROOT);
        return TABLE_DELETED_ERROR_MARKERS.stream().anyMatch(normalized::contains);
    }

    public static MetricsConstants.MetadataUploadFailureReasons getMetadataExtractorFailureReason(
        Throwable ex,
        MetricsConstants.MetadataUploadFailureReasons defaultReason){

        if (ex.getCause() instanceof RateLimitException){
            return MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING;
        }

        if (ex.getCause() instanceof NoSuchKeyException) {
          return MetricsConstants.MetadataUploadFailureReasons.NO_SUCH_KEY;
        }

        if (ex.getCause() instanceof AccessDeniedException){
            return MetricsConstants.MetadataUploadFailureReasons.ACCESS_DENIED;
        }

        return defaultReason;
    }
}
