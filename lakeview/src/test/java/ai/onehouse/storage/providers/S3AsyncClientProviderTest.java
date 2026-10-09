package ai.onehouse.storage.providers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.S3Config;
import ai.onehouse.config.models.configv1.ConfigV1;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

@ExtendWith(MockitoExtension.class)
class S3AsyncClientProviderTest {
  @Mock private ConfigV1 config;
  @Mock private FileSystemConfiguration fileSystemConfiguration;
  @Mock private MetadataExtractorConfig metadataExtractorConfig;
  @Mock private S3Config s3Config;
  @Mock private ExecutorService executorService;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  StsClient stsClient;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  StsClientBuilder stsClientBuilder;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  AssumeRoleResponse assumeRoleResponse;

  @Test
  void throwExceptionWhenS3ConfigIsNull() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(null);
    S3AsyncClientProvider clientProvider = new S3AsyncClientProvider(config, executorService);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createS3AsyncClient);

    assertEquals("S3 Config not found", thrown.getMessage());
  }

  @Test
  void throwExceptionWhenRegionIsBlank() {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    when(s3Config.getRegion()).thenReturn("");

    S3AsyncClientProvider clientProvider = new S3AsyncClientProvider(config, executorService);
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, clientProvider::createS3AsyncClient);

    assertEquals("Aws region cannot be empty", thrown.getMessage());
  }

  @ParameterizedTest
  @MethodSource("generateTestCases")
  void testCreateS3AsyncClientWithCredentialsWhenProvided(boolean isAssumedRoleFlow, boolean isRefreshSession) {
    when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    when(fileSystemConfiguration.getS3Config()).thenReturn(s3Config);
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getObjectStoreNumRetries()).thenReturn(10);
    when(metadataExtractorConfig.getNettyMaxConcurrency()).thenReturn(50);
    when(metadataExtractorConfig.getNettyConnectionTimeoutSeconds()).thenReturn(60L);
    if (isAssumedRoleFlow) {
      when(s3Config.getArnToImpersonate()).thenReturn(Optional.of("arn:aws:iam::396675327081:role/test-role"));
    } else {
      when(s3Config.getAccessSecret()).thenReturn(Optional.of("access-secret"));
      when(s3Config.getAccessKey()).thenReturn(Optional.of("access-key"));
    }
    when(s3Config.getRegion()).thenReturn("us-west-2");

    try (MockedStatic<StsClient> mockedStatic = Mockito.mockStatic(StsClient.class)) {
      if (isAssumedRoleFlow) {
        mockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
        when(StsClient.builder().region(any()).build()).thenReturn(stsClient);
        when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);
        when(assumeRoleResponse.credentials().accessKeyId()).thenReturn("access-key");
        when(assumeRoleResponse.credentials().secretAccessKey()).thenReturn("access-secret");
        when(assumeRoleResponse.credentials().sessionToken()).thenReturn("session-token");
      }

      S3AsyncClientProvider s3AsyncClientProviderSpy =
          Mockito.spy(new S3AsyncClientProvider(config, executorService));
      S3AsyncClientProvider.resetS3AsyncClient();
      if (!isRefreshSession) {
        S3AsyncClient result = s3AsyncClientProviderSpy.getS3AsyncClient();
        assertNotNull(result);
      } else {
        s3AsyncClientProviderSpy.refreshClient();
      }

      if (isAssumedRoleFlow) {
        verify(stsClient, times(1)).assumeRole(any(AssumeRoleRequest.class));
      }
      verify(s3AsyncClientProviderSpy, times(1)).createS3AsyncClient();
    }
  }

  static Stream<Arguments> generateTestCases() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, false),
        Arguments.of(false,true));
  }

  private S3AsyncClientProvider providerFor(S3Config realS3Config) {
    lenient().when(config.getFileSystemConfiguration()).thenReturn(fileSystemConfiguration);
    lenient().when(fileSystemConfiguration.getS3Config()).thenReturn(realS3Config);
    lenient().when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    lenient().when(metadataExtractorConfig.getObjectStoreNumRetries()).thenReturn(0);
    lenient().when(metadataExtractorConfig.getNettyMaxConcurrency()).thenReturn(10);
    lenient().when(metadataExtractorConfig.getNettyConnectionTimeoutSeconds()).thenReturn(5L);
    return new S3AsyncClientProvider(config, Executors.newSingleThreadExecutor());
  }

  @Test
  void doesNotSetEndpointOrPathStyleWhenAbsent() {
    S3AsyncClientProvider provider =
        providerFor(S3Config.builder().region("us-west-2").build());
    S3AsyncClientBuilder builder = mock(S3AsyncClientBuilder.class, Answers.RETURNS_SELF);
    doReturn(mock(S3AsyncClient.class)).when(builder).build();

    try (MockedStatic<S3AsyncClient> mockedStatic = Mockito.mockStatic(S3AsyncClient.class)) {
      mockedStatic.when(S3AsyncClient::builder).thenReturn(builder);
      provider.createS3AsyncClient();
    }

    verify(builder, never()).endpointOverride(any());
    verify(builder, never()).forcePathStyle(any());
    verify(builder, never()).serviceConfiguration(any(S3Configuration.class));
  }

  @Test
  void setsEndpointAndPathStyleWhenConfigured() {
    S3AsyncClientProvider provider =
        providerFor(
            S3Config.builder()
                .region("us-east-1")
                .endpoint(Optional.of("https://s3.example.internal:8333"))
                .pathStyleAccess(true)
                .build());
    S3AsyncClientBuilder builder = mock(S3AsyncClientBuilder.class, Answers.RETURNS_SELF);
    doReturn(mock(S3AsyncClient.class)).when(builder).build();

    try (MockedStatic<S3AsyncClient> mockedStatic = Mockito.mockStatic(S3AsyncClient.class)) {
      mockedStatic.when(S3AsyncClient::builder).thenReturn(builder);
      provider.createS3AsyncClient();
    }

    verify(builder).endpointOverride(URI.create("https://s3.example.internal:8333"));
    verify(builder).forcePathStyle(true);
  }

  @Test
  void resolvedClientUsesEndpointOverride() {
    S3AsyncClient client =
        providerFor(
                S3Config.builder()
                    .region("us-east-1")
                    .endpoint(Optional.of("https://s3.example.internal:8333"))
                    .build())
            .createS3AsyncClient();
    try {
      assertEquals(
          Optional.of(URI.create("https://s3.example.internal:8333")),
          client.serviceClientConfiguration().endpointOverride());
    } finally {
      client.close();
    }
  }

  @Test
  void resolvedClientHasNoEndpointOverrideWhenAbsent() {
    S3AsyncClient client =
        providerFor(S3Config.builder().region("us-east-1").build()).createS3AsyncClient();
    try {
      assertFalse(client.serviceClientConfiguration().endpointOverride().isPresent());
    } finally {
      client.close();
    }
  }

  @Test
  void sendsPathStyleRequestsToEndpoint() throws Exception {
    AtomicReference<String> requestPath = new AtomicReference<>();
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestPath.set(exchange.getRequestURI().getPath());
          hostHeader.set(exchange.getRequestHeaders().getFirst("Host"));
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
    int port = server.getAddress().getPort();
    S3AsyncClient client =
        providerFor(
                S3Config.builder()
                    .region("us-east-1")
                    .accessKey(Optional.of("access-key"))
                    .accessSecret(Optional.of("access-secret"))
                    .endpoint(Optional.of("http://127.0.0.1:" + port))
                    .pathStyleAccess(true)
                    .build())
            .createS3AsyncClient();
    try {
      client
          .headObject(HeadObjectRequest.builder().bucket("my-bucket").key("a/b.txt").build())
          .get(30, TimeUnit.SECONDS);
    } finally {
      client.close();
      server.stop(0);
    }

    assertEquals("/my-bucket/a/b.txt", requestPath.get());
    assertEquals("127.0.0.1:" + port, hostHeader.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "s3.example.internal:8333", "ftp://host", "https://", "not a url"})
  void rejectsInvalidEndpoint(String endpoint) {
    S3AsyncClientProvider provider =
        providerFor(
            S3Config.builder().region("us-east-1").endpoint(Optional.of(endpoint)).build());

    assertThrows(IllegalArgumentException.class, provider::createS3AsyncClient);
  }
}
