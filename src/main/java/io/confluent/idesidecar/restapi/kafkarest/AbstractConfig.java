package io.confluent.idesidecar.restapi.kafkarest;

import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;

public interface AbstractConfig {
  String name();
  String value();
  boolean isDefault();
  boolean isReadOnly();
  boolean isSensitive();
  ConfigEntry.ConfigSource source();
  List<ConfigSynonym> synonyms();

  record ConfigSynonym(
      String name,
      String value,
      ConfigEntry.ConfigSource source
  ) {

  }
}
