package ai.onehouse.api;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AsyncHttpClientWithRetryTest {

  private MockWebServer mockWebServer;
  private AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private OkHttpClient okHttpClient;

  @BeforeEach
  void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();

    okHttpClient = new OkHttpClient.Builder().build();
    asyncHttpClientWithRetry = new AsyncHttpClientWithRetry(3, 100, okHttpClient);
  }

  @AfterEach
  void tearDown() throws IOException {
    mockWebServer.shutdown();
    asyncHttpClientWithRetry.shutdownScheduler();
    assertEquals(0, okHttpClient.connectionPool().connectionCount());
    assertTrue(okHttpClient.dispatcher().executorService().isShutdown());
  }

  @Test
  void testRetryInterceptorRetriesOnFailure() throws InterruptedException, ExecutionException {
    // Set up a sequence of responses: two failures followed by a success
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200));

    Request request = new Request.Builder().url(mockWebServer.url("/")).get().build();

    CompletableFuture<Response> future = asyncHttpClientWithRetry.makeRequestWithRetry(request);
    Response response = future.get();

    assertTrue(response.isSuccessful());
    assertEquals(3, mockWebServer.getRequestCount());
  }

  @Test
  void testMakeRequestWithAllRetriesFail() throws InterruptedException, ExecutionException {
    // All responses are failures.
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));

    Request request = new Request.Builder().url(mockWebServer.url("/")).get().build();

    CompletableFuture<Response> future = asyncHttpClientWithRetry.makeRequestWithRetry(request);

    Response response = future.get();
    assertFalse(response.isSuccessful());
    assertEquals(3, mockWebServer.getRequestCount());
  }

  @Test
  void testMakeRequestWithRetryIOException() throws IOException {
    // Shut down the server to simulate an IOException.
    mockWebServer.shutdown();

    Request request = new Request.Builder().url("http://localhost:8080").get().build();

    CompletableFuture<Response> future = asyncHttpClientWithRetry.makeRequestWithRetry(request);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertInstanceOf(IOException.class, exception.getCause());

    assertEquals(0, mockWebServer.getRequestCount());
  }
}
