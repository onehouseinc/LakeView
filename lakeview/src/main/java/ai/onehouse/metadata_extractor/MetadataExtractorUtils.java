package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.RateLimitException;

import java.util.Locale;

public final class MetadataExtractorUtils {

    // The control plane sends this per-table error on a successful response for a deleted table.
    private static final String TABLE_DELETED_ERROR_MARKER = "skipped as it is deleted";

    private MetadataExtractorUtils(){}

    /**
     * Returns true if a per-table initialize-tables error is the "table is deleted" skip advisory.
     * The response has no structured error code, so this matches on the message text.
     */
    public static boolean isTableDeletedError(String error) {
        if (error == null) {
            return false;
        }
        return error.toLowerCase(Locale.ROOT).contains(TABLE_DELETED_ERROR_MARKER);
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
