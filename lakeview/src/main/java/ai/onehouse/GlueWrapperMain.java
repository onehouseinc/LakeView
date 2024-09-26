package ai.onehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.api.java.UDF1;

public class GlueWrapperMain implements UDF1<String, String> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String call(String argJson) throws Exception {
    String[] argList = MAPPER.readValue(argJson, String[].class);
    Main.main(argList);
    return "SUCCESS";
  }

  public static void main(String[] args) throws Exception {
    GlueWrapperMain wrapperMain = new GlueWrapperMain();
    wrapperMain.call(args[0]);
  }
}
