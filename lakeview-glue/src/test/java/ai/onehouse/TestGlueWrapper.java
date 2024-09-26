package ai.onehouse;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGlueWrapper {

  @Test
  void testGlueWrapper() {
    GlueWrapperMain wrapperMain = new GlueWrapperMain();
    String argsJson = "[\"-h\"]";

    Assertions.assertDoesNotThrow(() -> wrapperMain.call(argsJson));
  }
}
