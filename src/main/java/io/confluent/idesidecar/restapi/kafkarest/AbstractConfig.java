package io.confluent.idesidecar.restapi.kafkarest;

import io.soabase.recordbuilder.core.RecordInterface;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.List;

@RecordInterface
public interface AbstractConfig {
  String name();
  String value();
  boolean isDefault();
  boolean isReadOnly();
  boolean isSensitive();
  ConfigSource source();
  List<ConfigSynonym> synonyms();

  record ConfigSynonym(
      String name,
      String value,
      ConfigSource source
  ) {

  }
}
