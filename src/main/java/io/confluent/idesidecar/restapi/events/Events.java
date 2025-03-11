package io.confluent.idesidecar.restapi.events;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.util.AnnotationLiteral;

/**
 * Definition of <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI event</a> and
 * utilities. Quarkus CDI events is built on top of
 * <a href="https://docs.jboss.org/weld/reference/latest/en-US/html/events.html">Weld events</a>
 * (more in depth documentation).
 *
 * <h2>Producers and Consumers</h2>
 *
 * <p>Event <i>producers</i> fire events using the {@link Event} channel mechanism, which are
 * {@link jakarta.inject.Inject @Inject}-ed into the bean that is producing events. The type of
 * {@link Event} channel dictates the kind of events that can be emitted.
 *
 * <p>Event <i>consumers</i> use the {@link jakarta.enterprise.event.Observes} annotation on
 * methods that are to consume events fired by producers. Each consumer method handles a specific
 * kind of event.
 *
 * <p>This event mechanism decouples producers and consumers: a producer can emit events without
 * regard for which if any components will consume them, while consumers can consume events without
 * regard for the components that are producing the events.
 *
 * <h2>Qualifiers</h2>
 *
 * <p>Qualifiers are annotations that can be emitted with each event to add information about the
 * event. Qualifiers can represent any information, and are independent of the event type. But
 * typically qualifiers augment the event with additional information useful to consumers.
 *
 * <h3>Reusable Qualifiers</h3>
 * This package defines several qualifiers that can be reused on different event types.
 *
 * <p>The {@link Lifecycle} qualifiers represent different states within an
 * objects lifecycle, though not all these qualifiers may apply to states in the lifecycle of an
 * object:
 * <ul>
 *   <li>{@link Lifecycle.Created}</li>
 *   <li>{@link Lifecycle.Updated}</li>
 *   <li>{@link Lifecycle.Connected}</li>
 *   <li>{@link Lifecycle.Disconnected}</li>
 *   <li>{@link Lifecycle.Deleted}</li>
 * </ul>
 *
 * <p>The {@link ServiceKind} qualifiers represent different types of connections:
 * <ul>
 *   <li>{@link ServiceKind.CCloud}</li>
 *   <li>{@link ServiceKind.ConfluentPlatform}</li>
 *   <li>{@link ServiceKind.Local}</li>
 * </ul>
 *
 * <p>The {@link ClusterKind} qualifiers represent different types of clusters:
 * <ul>
 *   <li>{@link ClusterKind.Kafka}</li>
 *   <li>{@link ClusterKind.SchemaRegistry}</li>
 * </ul>
 *
 * <p>Other qualifiers can be added.
 *
 * <h3>Consumers and Qualifiers</h3>
 *
 * <p>Consumers can also optionally use qualifiers to filter the kinds of events they want to see.
 *
 * <p>A consumer method that does not include any qualifiers will receive all events.
 * Typically, consumer methods use either no qualifiers (receive all events) or
 * a single qualifier to receive events with that qualifier.
 *
 * <p>It is possible for a consumer method to use multiple qualifiers, but the method is called
 * if its set of qualifiers is a subset of the fired event's qualifiers -- in other words,
 * all method qualifiers exist on the event. This is likely useful when the qualifiers
 * are orthogonal (e.g., one lifecycle qualifiers and another connection type qualifier).
 */
public class Events {

  /**
   * Qualifiers that deal with an object's lifecycle. Objects do not need to have states that
   * correspond to these qualifiers.
   */
  public static class LifecycleQualifier {

    /**
     * Return a qualifier to signal the annotated event represents an object that was created.
     */
    public static AnnotationLiteral<Lifecycle.Created> created() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object that was updated.
     */
    public static AnnotationLiteral<Lifecycle.Updated> updated() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object that was deleted.
     */
    public static AnnotationLiteral<Lifecycle.Deleted> deleted() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object that became
     * "connected".
     */
    public static AnnotationLiteral<Lifecycle.Connected> connected() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object that became
     * "disconnected".
     */
    public static AnnotationLiteral<Lifecycle.Disconnected> disconnected() {
      return new AnnotationLiteral<>() {
      };
    }
  }

  /**
   * Qualifiers that deal with the type of connection.
   */
  public static class ConnectionTypeQualifier {

    /**
     * Return a qualifier to signal the annotated event represents an object from
     * {@link ConnectionType#CCLOUD Confluent Cloud}.
     */
    public static AnnotationLiteral<ServiceKind.CCloud> ccloud() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object from
     * {@link ConnectionType#PLATFORM Confluent Platform}.
     */
    public static AnnotationLiteral<ServiceKind.ConfluentPlatform> confluentPlatform() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents a
     * {@link ConnectionType#DIRECT Direct} connection object
     */
    public static AnnotationLiteral<ServiceKind.Direct> direct() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Return a qualifier to signal the annotated event represents an object from
     * {@link ConnectionType#LOCAL lcoal services}.
     */
    public static AnnotationLiteral<ServiceKind.Local> local() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * Utility method to create the correct {@link AnnotationLiteral qualfiier} for the
     * {@link ConnectionType} of the given {@link ConnectionState}.
     */
    public static AnnotationLiteral<?> typeQualifier(ConnectionState state) {
      return typeQualifier(state.getType());
    }

    /**
     * Utility method to create the correct {@link AnnotationLiteral qualfiier} for the given
     * {@link ConnectionType}.
     */
    public static AnnotationLiteral<?> typeQualifier(ConnectionType type) {
      return switch (type) {
        case LOCAL -> ConnectionTypeQualifier.local();
        case CCLOUD -> ConnectionTypeQualifier.ccloud();
        case DIRECT -> ConnectionTypeQualifier.direct();
        case PLATFORM -> ConnectionTypeQualifier.confluentPlatform();
      };
    }
  }

  /**
   * Qualifiers that deal with the type of cluster.
   */
  public static class ClusterTypeQualifier {

    /**
     * Return a qualifier to signal the annotated event represents a
     * {@link ClusterKind.Kafka Kafka cluster}.
     */
    public static AnnotationLiteral<ClusterKind.Kafka> kafkaCluster() {
      return new AnnotationLiteral<>() {
      };
    }

    /**
     * QReturn a qualifier to signal the annotated event represents a
     * {@link ClusterKind.SchemaRegistry Schema Registry}.
     */
    public static AnnotationLiteral<ClusterKind.SchemaRegistry> schemaRegistry() {
      return new AnnotationLiteral<>() {
      };
    }
  }

  /**
   * Utility method to asynchronously fire the given event with qualifiers using the specified
   * {@link Event} channel. This method does not wait for consumers.
   *
   * @param channel    the event channel to which the event should be emitted
   * @param event      the event to emit
   * @param qualifiers the zero or more qualifiers that are to be emitted with the event
   * @param <EventT>   the type of event
   */
  public static <EventT> void fireAsyncEvent(
      Event<EventT> channel,
      EventT event,
      AnnotationLiteral<?>... qualifiers
  ) {
    if (event == null) {
      // do nothing with a null event
      return;
    }
    // Add any qualifiers
    for (AnnotationLiteral<?> qualifier : qualifiers) {
      if (qualifier != null) {
        channel = channel.select(qualifier);
      }
    }
    // and fire the event
    channel.fireAsync(event);
  }

  /**
   * Utility method to synchronously fire the given event using the specified {@link Event} channel.
   * This method waits for consumers.
   *
   * @param channel  the event channel to which the event should be emitted
   * @param event    the event to emit
   * @param <EventT> the type of event
   */
  public static <EventT> void fireSyncEvent(
      Event<EventT> channel,
      EventT event
  ) {
    if (event == null) {
      // do nothing with a null event
      return;
    }
    // and fire the event
    channel.fire(event);
  }
}
