package com.onehouse.cli_parser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliParser {
  private String configFilePath;
  private String configYamlString;
  private static final String PATH_OPTION = "p";
  private static final String CONFIG_OPTION = "c";

  public void parse(String[] args) throws ParseException {
    Options options = new Options();

    Option pathOption =
        Option.builder(PATH_OPTION)
            .longOpt("path")
            .hasArg()
            .desc("The file path to the configuration file")
            .build();
    options.addOption(pathOption);

    Option configOption =
        Option.builder(CONFIG_OPTION)
            .longOpt("config")
            .hasArg()
            .desc("The YAML configuration string")
            .build();
    options.addOption(configOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(PATH_OPTION) && cmd.hasOption(CONFIG_OPTION)) {
      throw new ParseException("Cannot specify both a file path and a config string.");
    }

    if (cmd.hasOption(PATH_OPTION)) {
      configFilePath = cmd.getOptionValue(PATH_OPTION);
    }

    if (cmd.hasOption(CONFIG_OPTION)) {
      configYamlString = cmd.getOptionValue(CONFIG_OPTION);
    }
  }

  public String getConfigFilePath() {
    return configFilePath;
  }

  public String getConfigYamlString() {
    return configYamlString;
  }
}
