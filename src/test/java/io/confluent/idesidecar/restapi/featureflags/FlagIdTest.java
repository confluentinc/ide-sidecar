package io.confluent.idesidecar.restapi.featureflags;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FlagIdTest {

  @Test
  void shouldNotHaveDuplicates() {
    Set<String> ids = new HashSet<>();
    for (FlagId id : FlagId.values()) {
      assertTrue(
          ids.add(id.toString()),
          "The FlagId %s has an ID '%s' that duplicates another".formatted(id.name(), id)
      );
    }
  }

  @Test
  void shouldHaveDefaultsForAllFlagIds() {
    var defaults = FlagEvaluations.defaults();
    for (FlagId id : FlagId.values()) {
      var evaluation = defaults.evaluation(id.toString());
      assertTrue(
          evaluation.isPresent(),
          "The FlagId %s('%s') does not have a default in the resource file 'src/main/resources/%s'".formatted(
              id.name(),
              id,
              FlagEvaluations.DEFAULT_FLAG_EVALUATIONS_RESOURCE
          )
      );
    }
  }
}
