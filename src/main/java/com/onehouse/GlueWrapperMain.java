package com.onehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.api.java.UDF1;

public class GlueWrapperMain implements UDF1<String, Void> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public Void call(String argJson) throws Exception {
    String[] argList = MAPPER.readValue(argJson, String[].class);
    Main.main(argList);
    return null;
  }

  /**
   * Only for testing, to be removed.
   * @param args .
   * @throws Exception .
   */
  public static void main(String[] args) throws Exception {
    GlueWrapperMain wrapperMain = new GlueWrapperMain();
    String argsJson = "[\"-h\"]";
    wrapperMain.call(argsJson);
  }
}
