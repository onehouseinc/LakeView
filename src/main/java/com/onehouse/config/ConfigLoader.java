package com.onehouse.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onehouse.config.models.common.OnehouseClientConfig;
import com.onehouse.config.models.configv1.ConfigV1;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

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

  private Config loadConfigFromJsonNode(JsonNode jsonNode) throws IOException {
    ConfigVersion version = ConfigVersion.valueOf(jsonNode.get("version").asText());
    switch (version) {
      case V1:
        ConfigV1 configV1 = MAPPER.treeToValue(jsonNode, ConfigV1.class);
        if (StringUtils.isNotBlank(configV1.getOnehouseClientConfig().getFile())) {
          String onehouseClientConfigFileContent =
              new String(
                  Files.readAllBytes(Paths.get(configV1.getOnehouseClientConfig().getFile())));
          OnehouseClientConfig onehouseClientConfigFromFile =
              MAPPER.readValue(onehouseClientConfigFileContent, OnehouseClientConfig.class);
          configV1
              .getOnehouseClientConfig()
              .setProjectId(onehouseClientConfigFromFile.getProjectId());
          configV1.getOnehouseClientConfig().setApiKey(onehouseClientConfigFromFile.getApiKey());
          configV1
              .getOnehouseClientConfig()
              .setApiSecret(onehouseClientConfigFromFile.getApiSecret());
          configV1.getOnehouseClientConfig().setUserId(onehouseClientConfigFromFile.getUserId());
        }
        validateOnehouseClientConfig(configV1);
        return configV1;
      default:
        throw new UnsupportedOperationException("Unsupported config version: " + version);
    }
  }

  private void validateOnehouseClientConfig(ConfigV1 configV1) {
    @NonNull OnehouseClientConfig onehouseClientConfig = configV1.getOnehouseClientConfig();
    List<String> missingFields = new ArrayList<>();
    if (StringUtils.isBlank(onehouseClientConfig.getProjectId())) {
      missingFields.add("projectId");
    }
    if (StringUtils.isBlank(onehouseClientConfig.getApiKey())) {
      missingFields.add("apiKey");
    }
    if (StringUtils.isBlank(onehouseClientConfig.getApiSecret())) {
      missingFields.add("apiSecret");
    }
    if (StringUtils.isBlank(onehouseClientConfig.getUserId())) {
      missingFields.add("userId");
    }
    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Missing config params: %s",
              missingFields.stream().reduce((a, b) -> a + ", " + b).orElse("")));
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
