package com.onehouse.api;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import okhttp3.*;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetryInterceptorTest {

  private MockWebServer mockWebServer;
  private OkHttpClient okHttpClient;

  @BeforeEach
  void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();

    okHttpClient = new OkHttpClient.Builder().addInterceptor(new RetryInterceptor(3, 100)).build();
  }

  @AfterEach
  void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  void testRetryInterceptor_retriesOnFailure() throws IOException {
    // Set up a sequence of responses: two failures followed by a success
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200));

    Request request = new Request.Builder().url(mockWebServer.url("/")).get().build();

    // Execute the request
    Call call = okHttpClient.newCall(request);
    Response response = call.execute();

    // Assert that the final response is successful
    assertTrue(response.isSuccessful());

    // Assert that the request was issued three times
    assertEquals(3, mockWebServer.getRequestCount());
  }
}
