package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.RateLimitException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;
import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.isTableDeletedError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class MetadataExtractorUtilsTest {
    @ParameterizedTest
    @MethodSource("provideTestCases")
    void testGetMetadataExtractorFailureReason(MetricsConstants.MetadataUploadFailureReasons reasons,
                                               MetricsConstants.MetadataUploadFailureReasons expected,
                                               Throwable throwable) {
        MetricsConstants.MetadataUploadFailureReasons reason = getMetadataExtractorFailureReason(
            new CompletionException(throwable),
            reasons
        );
        assertEquals(expected, reason);
    }

    static Stream<Arguments> provideTestCases() {
        return Stream.of(
            Arguments.of(MetricsConstants.MetadataUploadFailureReasons.UNKNOWN,
                MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING,
                new RateLimitException("")),
            Arguments.of(MetricsConstants.MetadataUploadFailureReasons.NO_SUCH_KEY,
                MetricsConstants.MetadataUploadFailureReasons.NO_SUCH_KEY,
                new NoSuchKeyException("")),
            Arguments.of(MetricsConstants.MetadataUploadFailureReasons.HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED,
                MetricsConstants.MetadataUploadFailureReasons.HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED,
                new RuntimeException("")),
            Arguments.of(MetricsConstants.MetadataUploadFailureReasons.UNKNOWN,
                MetricsConstants.MetadataUploadFailureReasons.ACCESS_DENIED,
                new AccessDeniedException("")));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // Verbatim control-plane advisory observed in production (PD Q043G44USWZC7F).
        "Table can be skipped as it is deleted",
        // Same advisory as it reaches the metrics/log call site, wrapped with table context.
        "Error initialising table Table(absoluteTableUri=s3://bucket/db/tbl/v1, databaseName=db, "
            + "tableId=ca61e51d-9080-324f-b56c-765ffca993eb, tableVersion=6, tableFormat=HUDI): "
            + "Table can be skipped as it is deleted",
        // Case-insensitive.
        "TABLE CAN BE SKIPPED AS IT IS DELETED",
        // Tolerates a reworded prefix around the distinctive core.
        "This table can be skipped as it is deleted",
        "table is deleted",
    })
    void testIsTableDeletedErrorMatchesDeletedAdvisory(String error) {
        assertTrue(isTableDeletedError(error));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // Real failures must NOT be reclassified as a benign skip.
        "API call failed with status code: 401 - Confirm that your API token is valid and has not expired.",
        "API call failed with status code: 403 - Forbidden",
        "Internal server error",
        // Mentions deletion but is a genuine failure, not the skip advisory.
        "Failed to delete table metadata",
        "could not determine whether table was deleted",
    })
    void testIsTableDeletedErrorDoesNotMatchRealFailures(String error) {
        assertFalse(isTableDeletedError(error));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testIsTableDeletedErrorHandlesNullAndEmpty(String error) {
        assertFalse(isTableDeletedError(error));
    }
}
