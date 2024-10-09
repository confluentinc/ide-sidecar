package io.confluent.idesidecar.restapi.featureflags;

import io.smallrye.common.constraint.NotNull;
import java.util.Locale;

/**
 * An identifier for a feature flag.
 */
public enum FlagId {

  IDE_GLOBAL_ENABLE("ide.global.enable"),
  IDE_GLOBAL_NOTICES("ide.global.notices"),
  IDE_CCLOUD_ENABLE("ide.ccloud.enable"),
  IDE_SENTRY_ENABLED("ide.sentry.enabled");

  private final String id;

  FlagId(@NotNull String id) {
    this.id = id.toLowerCase(Locale.ENGLISH);
  }

  public String id() {
    return id;
  }

  @Override
  public String toString() {
    return id();
  }
}
