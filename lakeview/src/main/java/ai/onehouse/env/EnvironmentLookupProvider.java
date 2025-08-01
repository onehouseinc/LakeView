package ai.onehouse.env;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@FunctionalInterface
public interface EnvironmentLookupProvider {

  @Nullable
  String getValue(@Nonnull String key);

  class System implements EnvironmentLookupProvider {
    @Nullable @Override
    public String getValue(@Nonnull String key) {
      return java.lang.System.getenv(key);
    }
  }
}
