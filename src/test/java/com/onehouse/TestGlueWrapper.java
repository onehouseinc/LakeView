package com.onehouse;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

public class TestGlueWrapper {

  @Test
  void testGlueWrapper() {
    GlueWrapperMain wrapperMain = new GlueWrapperMain();
    String argsJson = "[\"-h\"]";

    assertDoesNotThrow(() -> wrapperMain.call(argsJson));
  }
}
