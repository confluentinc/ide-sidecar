package io.confluent.idesidecar.restapi.events;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Qualifiers of different lifecycle states.
 * Consumer methods can use these annotations to be called only with the annotation.
 */
@Qualifier
@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
public @interface Lifecycle {

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Created {}

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Updated {}

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Deleted {}

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Connected {}

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Disconnected {}

}
