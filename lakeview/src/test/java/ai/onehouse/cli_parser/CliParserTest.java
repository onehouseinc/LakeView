package ai.onehouse.cli_parser;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.Test;

class CliParserTest {

  @Test
  void testParsePathOption() throws ParseException {
    CliParser parser = new CliParser();
    String[] args = {"-p", "config.yaml"};
    parser.parse(args);
    assertEquals("config.yaml", parser.getConfigFilePath());
    assertNull(parser.getConfigYamlString());
  }

  @Test
  void testParseConfigOption() throws ParseException {
    CliParser parser = new CliParser();
    String[] args = {"-c", "config: value"};
    parser.parse(args);
    assertEquals("config: value", parser.getConfigYamlString());
    assertNull(parser.getConfigFilePath());
  }

  @Test
  void testParseBothOptions() {
    CliParser parser = new CliParser();
    String[] args = {"-p", "config.yaml", "-c", "config: value"};

    Exception exception =
        assertThrows(
            ParseException.class,
            () -> {
              parser.parse(args);
            });

    String expectedMessage = "Cannot specify both file path and config string.";
    String actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  void testHelpOption() throws ParseException {
    CliParser parser = new CliParser();
    String[] args = {"-h"};
    parser.parse(args);
    assertTrue(parser.isHelpRequested());
  }

  @Test
  void testNoOptions() throws ParseException {
    CliParser parser = new CliParser();
    String[] args = {};
    parser.parse(args);
    assertFalse(parser.isHelpRequested());
    assertNull(parser.getConfigFilePath());
    assertNull(parser.getConfigYamlString());
  }
}
