package com.onehouse;

import static org.junit.jupiter.api.Assertions.*;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class GlueWrapperMainTest {

  @Test
  @SneakyThrows
  public void testMain() {
    try {
      GlueWrapperMain.main(new String[] {"[\"-h\"]"});
    } catch (Exception e) {
      fail(e);
    }
  }
}
