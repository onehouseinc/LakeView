package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.RateLimitException;

public final class MetadataExtractorUtils {

    private MetadataExtractorUtils(){}

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
