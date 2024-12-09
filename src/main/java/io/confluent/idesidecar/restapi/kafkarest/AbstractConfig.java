package io.confluent.idesidecar.restapi.kafkarest;

import java.util.List;
import org.apache.kafka.clients.admin.ConfigEntry;

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
