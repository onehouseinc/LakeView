package com.onehouse;

import com.onehouse.cli_parser.CliParser;
import com.onehouse.config.ConfigLoader;
import org.apache.spark.sql.api.java.UDF1;

public class GlueWrapperMain implements UDF1<Integer, Integer> {
  @Override
  public Integer call(Integer number) throws Exception {
    CliParser parser = new CliParser();
    ConfigLoader configLoader = new ConfigLoader();

    Main main = new Main(parser, configLoader);
    main.print();

    return number * number;
  }
}
