package io.confluent.idesidecar.restapi.featureflags;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class ScheduledRefreshPredicateTest {

  @Test
  void shouldInstantiateScheduledRefreshPredicate() {
    var predicate = new ScheduledRefreshPredicate();
    assertNotNull(predicate);
    predicate.test(null);
  }

}