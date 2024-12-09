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
 * Qualifiers of different kinds of services. Consumer methods can use these annotations to be
 * called only with the annotation.
 */
@Qualifier
@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
public @interface ServiceKind {

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface CCloud {

  }

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface ConfluentPlatform {

  }

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Direct {

  }

  @Qualifier
  @Target({METHOD, FIELD, PARAMETER, TYPE})
  @Retention(RUNTIME)
  @interface Local {

  }

}
