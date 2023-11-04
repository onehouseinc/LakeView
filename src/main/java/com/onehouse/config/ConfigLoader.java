package com.onehouse.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onehouse.config.configv1.ConfigV1;
import java.io.InputStream;

public class ConfigLoader {

  public Config loadConfig(String configFile) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.registerModule(new Jdk8Module());
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
}
