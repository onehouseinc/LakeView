package com.onehouse.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.onehouse.storage.AsyncStorageClient;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.core.io.ByteArrayResource;

@Slf4j
public class ConfigRefresher {
  private static final int POLL_PERIOD_MINUTES = 2;
  private final String baseConfig;
  private final String extractorConfigPath;
  private final AsyncStorageClient storageClient;
  private final ScheduledExecutorService executorService;
  private final ConfigProvider configProvider;
  private final ConfigLoader configLoader;

  public ConfigRefresher(
      String baseConfig,
      String extractorConfigPath,
      AsyncStorageClient storageClient,
      ConfigLoader configLoader,
      ConfigProvider configProvider) {
    this.baseConfig = baseConfig;
    this.extractorConfigPath = extractorConfigPath;
    this.storageClient = storageClient;
    this.executorService = Executors.newScheduledThreadPool(1);
    this.configProvider = configProvider;
    this.configLoader = configLoader;
  }

  public void start() throws Exception {
    fetchAndOverrideConfig();
    executorService.scheduleAtFixedRate(
        () -> {
          try {
            fetchAndOverrideConfig();
          } catch (Exception ex) {
            log.error("failed to fetch override config", ex);
          }
        },
        POLL_PERIOD_MINUTES,
        POLL_PERIOD_MINUTES,
        TimeUnit.MINUTES);
  }

  private void fetchAndOverrideConfig() throws JsonProcessingException {
    byte[] extractorConfigBytes = storageClient.readFileAsBytes(extractorConfigPath).join();
    Config newConfigWithOverride =
        mergeOverrideConfig(
            new ByteArrayResource(baseConfig.getBytes()),
            new ByteArrayResource(extractorConfigBytes));
    configProvider.setConfig(newConfigWithOverride);
  }

  private Config mergeOverrideConfig(ByteArrayResource... configs) throws JsonProcessingException {
    YamlMapFactoryBean factory = new YamlMapFactoryBean();
    factory.setResolutionMethod(YamlProcessor.ResolutionMethod.OVERRIDE);
    factory.setResources(configs);
    Map<String, Object> configValueMap = factory.getObject();
    JsonMapper jsonMapper = new JsonMapper();
    String finalConfigString = jsonMapper.writeValueAsString(configValueMap);
    return configLoader.loadConfigFromString(finalConfigString);
  }
}
