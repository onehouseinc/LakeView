package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.RateLimitException;

public final class MetadataExtractorUtils {

    private MetadataExtractorUtils(){}

    public static MetricsConstants.MetadataUploadFailureReasons getMetadataExtractorFailureReason(
        Throwable ex,
        MetricsConstants.MetadataUploadFailureReasons defaultReason){

        if (ex.getCause() instanceof RateLimitException){
            return MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING;
        }

        return defaultReason;
    }
}
