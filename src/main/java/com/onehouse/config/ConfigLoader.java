package com.onehouse.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onehouse.config.models.configv1.ConfigV1;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigLoader {
  private final ObjectMapper MAPPER;

  public ConfigLoader() {
    this.MAPPER = new ObjectMapper(new YAMLFactory());
    MAPPER.registerModule(new Jdk8Module());
  }

  public Config loadConfigFromConfigFile(String configFilePath) {
    try (InputStream in = Files.newInputStream(Paths.get(configFilePath))) {
      return loadConfigFromJsonNode(MAPPER.readTree(in));
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config", e);
    }
  }

  public Config loadConfigFromString(String configYaml) {
    try {
      return loadConfigFromJsonNode(MAPPER.readTree(configYaml));
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config", e);
    }
  }

  private Config loadConfigFromJsonNode(JsonNode jsonNode) throws JsonProcessingException {
    ConfigVersion version = ConfigVersion.valueOf(jsonNode.get("version").asText());
    switch (version) {
      case V1:
        return MAPPER.treeToValue(jsonNode, ConfigV1.class);
      default:
        throw new UnsupportedOperationException("Unsupported config version: " + version);
    }
  }

  public String convertConfigToString(Config config) throws JsonProcessingException {
    switch (config.getVersion()) {
      case V1:
        ConfigV1 configV1 = (ConfigV1) config;
        return MAPPER.writeValueAsString(configV1);
      default:
        throw new UnsupportedOperationException(
            "Unsupported config version: " + config.getVersion());
    }
  }
}
