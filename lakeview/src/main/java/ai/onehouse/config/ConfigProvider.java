package ai.onehouse.config;

import java.util.concurrent.atomic.AtomicReference;

public class ConfigProvider {
  private AtomicReference<Config> configRef;

  public ConfigProvider(Config config) {
    configRef = new AtomicReference<>();
    setConfig(config);
  }

  public Config getConfig() {
    return configRef.get();
  }

  public void setConfig(Config config) {
    configRef.set(config);
  }
}
