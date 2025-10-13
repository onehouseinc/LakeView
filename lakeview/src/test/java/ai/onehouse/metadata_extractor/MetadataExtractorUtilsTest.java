package ai.onehouse.metadata_extractor;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.RateLimitException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
