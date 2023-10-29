package com.onehouse.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;

public class ConfigLoader {

  public static Config loadConfig(String configFile) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try (InputStream in = ConfigLoader.class.getResourceAsStream(configFile)) {
      JsonNode rootNode = mapper.readTree(in);
      ConfigVersion version = ConfigVersion.valueOf(rootNode.get("version").asText());
      switch (version) {
        case V1:
          return mapper.treeToValue(rootNode, ConfigV1.class);
        default:
          throw new UnsupportedOperationException("Unsupported config version: " + version);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config", e);
    }
  }

  public static void main(String[] args) {
    Config config = loadConfig("/testConfig.yaml");
    System.out.println(config.getVersion());
  }
}
