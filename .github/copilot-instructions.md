# IDE Sidecar - AI Coding Agent Instructions

## Project Overview

IDE Sidecar is a **Java 21 Quarkus application** that serves as the backend for Confluent's VS Code extension. It exposes REST and GraphQL APIs for managing connections to Kafka clusters and consuming/producing messages. The project is compiled to a **native executable using GraalVM** for fast startup and low memory footprint.

**Key Connection Types:**
- `CCLOUD`: Confluent Cloud connections (OAuth-based authentication)
- `LOCAL`: Confluent Local/standalone Kafka
- `DIRECT`: Direct connections to Kafka clusters (supports multiple auth methods: Basic, SASL/SCRAM, Kerberos, OAuth, TLS)

## Architecture

### Processor Chain Pattern

Request handling uses a **chain-of-responsibility pattern** via the `Processor<T, U>` class. Processors are chained using `Processor.chain(...)`:

```java
// Example from MessageViewerProcessorBeanProducers
Processor.chain(
    new ConnectionProcessor<>(connectionManager),
    new KafkaClusterInfoProcessor<>(clusterCache),
    new ConsumeStrategyProcessor(nativeStrategy, ccloudStrategy),
    new EmptyProcessor<>()
)
```

Each processor:
1. Receives a context object (e.g., `KafkaRestProxyContext`, `ProxyContext`)
2. Performs its operation (auth, cluster lookup, proxying)
3. Passes the modified context to the next processor

**Find processor implementations in:** `src/main/java/io/confluent/idesidecar/restapi/processors/` and `src/main/java/io/confluent/idesidecar/restapi/proxy/`

### GraphQL API Structure

GraphQL APIs are defined with `@GraphQLApi` annotation:
- `ConfluentCloudQueryResource`: CCloud organizations, environments, Kafka clusters, Schema Registries
- `ConfluentLocalQueryResource`: Local Kafka and Schema Registry
- `DirectQueryResource`: Direct connection clusters

Schema generation happens automatically. Find generated schemas in `src/generated/resources/`.

### REST API Structure

REST endpoints use JAX-RS annotations (`@Path`, `@POST`, etc.):
- `ConnectionsResource`: CRUD for connections (`/gateway/v1/connections`)
- `KafkaConsumeResource`: Message viewer (`/gateway/v1/clusters/{cluster_id}/topics/{topic_name}/partitions/-/consume`)
- `ClusterRestProxyResource`: Proxies requests to Kafka/SR clusters

OpenAPI specs auto-generated to `src/generated/resources/openapi.{yaml,json}`.

### Connection State Management

`ConnectionStateManager` is the single source of truth for connection lifecycle:
- Uses CDI events for lifecycle notifications (Created, Updated, Connected, Disconnected, Deleted)
- Each `ConnectionState` wraps a `ConnectionSpec` with runtime state
- State changes fire events with qualifiers like `@Lifecycle.Connected` and `@ServiceKind.CCloud`

**Observer pattern**: Other components inject `Event<ConnectionState>` to react to connection changes.

## Development Workflows

### Building & Testing

**CRITICAL**: This project requires **Java 21** and **GraalVM CE**. Install via SDKMAN:
```bash
sdk env install  # Uses .sdkmanrc in project root
```

**Primary Make Commands:**
- `make quarkus-dev`: Dev mode with hot reload (port 26636)
- `make quarkus-test`: Continuous testing mode
- `make build`: Full build (compiles, tests, packages)
- `make test`: Run unit + integration tests (JVM mode)
- `make test-native`: Build native executable and run integration tests against it
- `make mvn-package-native`: Create native executable
- `make mvn-generate-sidecar-openapi-spec`: Regenerate OpenAPI specs

**Dev Mode URLs:**
- Application: `http://localhost:26636`
- Dev UI: `http://localhost:26636/q/dev-ui`
- GraphQL UI: `http://localhost:26636/q/graphql-ui`
- Swagger UI: `http://localhost:26636/swagger-ui`
- OpenAPI spec: `http://localhost:26636/openapi`

### Authentication in Development

The sidecar uses token-based auth. Get a token via handshake:
```bash
export DTX_ACCESS_TOKEN=$(curl -s http://localhost:26636/gateway/v1/handshake | jq -r .auth_secret)
```

Use in requests: `Authorization: Bearer ${DTX_ACCESS_TOKEN}`

In tests, use `@TestProfile(NoAccessFilterProfile.class)` to disable auth checks.

### Testing Conventions

**Unit tests** (`*Test.java`):
- Use `@QuarkusTest` for CDI/injection support
- Mock external dependencies
- Focus on isolated component behavior

**Integration tests** (`*IT.java`):
- Annotated with `@Tag("io.confluent.common.utils.IntegrationTest")`
- Use `@ConnectWireMock` for mocking external HTTP services (CCloud APIs, Kafka REST Proxy)
- Run against real Quarkus-managed containers via DevServices
- WireMock runs on `${quarkus.wiremock.devservices.port}` in tests

**Native integration tests**:
- Use profile `nativeit` (config in `src/test/resources/application-nativeit.yaml`)
- Run with `make test-native`

**Test utilities**: `SidecarClient` in `src/test/java/.../util/` provides helpers for GraphQL queries, connection creation, etc.

## Project-Specific Patterns

### Configuration

All config in `src/main/resources/application.yml`, with profile overrides:
- `%dev`: Development mode (port 26636, JSON logging off)
- `%test`: Test mode (uses WireMock, different ports to avoid conflicts)
- Production: Default values (runs from native executable)

**Important config sections:**
- `ide-sidecar.connections.ccloud.*`: CCloud API endpoints
- `ide-sidecar.connections.confluent-local.resources.*`: Local cluster discovery
- `ide-sidecar.connections.direct.*`: Direct connection settings
- `ide-sidecar.admin-client-configs.*`: Default Kafka AdminClient configs

### Dependency Injection

Uses Quarkus CDI (Jakarta EE). Common patterns:
- `@ApplicationScoped`: Singleton services
- `@Inject`: Field injection
- `@Named("beanName")`: Named producer beans (see `*BeanProducers` classes)
- `@Produces`: Producer methods in `*BeanProducers` classes

### Reactive Programming

Uses SmallRye Mutiny (`Uni<T>`, `Multi<T>`):
- `Uni<T>`: Async single result (like `CompletableFuture`)
- `Multi<T>`: Async stream
- Vert.x for HTTP clients and reactive routes

**Common pattern**: REST endpoints return `Uni<Response>` for non-blocking I/O.

### Message Viewer (Consumer) Implementation

Two strategies in `src/main/java/.../messageviewer/strategy/`:
- `NativeConsumeStrategy`: Direct Kafka Consumer for Local/Direct connections
- `ConfluentCloudConsumeStrategy`: Proxies to CCloud's internal consume API

Strategy selected in `ConsumeStrategyProcessor` based on connection type.

### Schema Registry Integration

`SchemaManager` handles schema lookup and validation:
- Supports Avro, Protobuf, JSON schemas
- Uses subject name strategies (TopicName, RecordName, TopicRecordName)
- `RecordDeserializer` deserializes consumed records based on schema

### Code Generation

**Protobuf**: `message.proto` generates Java classes via Maven plugin
**OpenAPI**: Auto-generated from JAX-RS annotations
**GraphQL Schema**: Auto-generated from `@GraphQLApi` resources

Regenerate OpenAPI: `make mvn-generate-sidecar-openapi-spec`

### Native Executable Considerations

When adding new features:
- Use `@RegisterForReflection` on classes accessed via reflection (e.g., DTOs, record classes)
- Test against native build with `make test-native` before PR
- Native build config in `pom.xml` under `native` profile
- GraalVM substitutions in `src/main/java/.../application/*NativeLoader.java` for native libs (Snappy, Zstd)

## Common Tasks

**Add a new REST endpoint:**
1. Create method in existing `*Resource` class or new class with `@Path`
2. Use `@APIResponseSchema` for OpenAPI docs
3. Add integration test in `src/test/java` with `@QuarkusTest` and `@ConnectWireMock`
4. Regenerate OpenAPI: `make mvn-generate-sidecar-openapi-spec`

**Add a new GraphQL query:**
1. Add method to `ConfluentCloudQueryResource`, `ConfluentLocalQueryResource`, or `DirectQueryResource`
2. Use `@Query` or field resolver (method on `@Source` parameter)
3. Add test using `SidecarClient.submitGraphQL()`
4. Schema auto-regenerates on build

**Add a new processor:**
1. Extend `Processor<T, U>` in `src/main/java/.../processors/` or `.../proxy/`
2. Implement `process(T context)` method
3. Chain in appropriate `*BeanProducers` class using `Processor.chain(...)`
4. Add unit tests mocking dependencies

**Add a new connection type configuration:**
1. Extend `ConnectionSpec` record with new config record
2. Add validation in the config record's `validate()` method
3. Update `ConnectionStateManager` to handle new type
4. Add fetcher implementation in `src/main/java/.../models/graph/`

## Important Files

- `pom.xml`: Maven config, Quarkus version 3.20.0, Java 21
- `Makefile` + `mk-files/*.mk`: Build automation
- `service.yml`: Confluent tooling config
- `src/main/java/io/confluent/idesidecar/restapi/`:
  - `connections/ConnectionStateManager.java`: Connection lifecycle
  - `models/ConnectionSpec.java`: Connection configuration DTOs
  - `processors/Processor.java`: Base processor class
  - `resources/*QueryResource.java`: GraphQL APIs
  - `resources/*Resource.java`: REST APIs
  - `application/*BeanProducers.java`: CDI producer beans

## Release Process

Every merge to `main` triggers Semaphore CI/CD:
- Builds native executables for Linux (AMD64, ARM64), macOS (AMD64, ARM64), Windows (x64)
- Creates GitHub release with executables as assets
- Used by confluentinc/vscode extension

**Do NOT include `[ci skip]` in commit messages** unless intentionally skipping CI.

## Security

- `pre-commit` hooks enforce secret scanning via `gitleaks`
- Install locally: `pre-commit install`
- NEVER commit secrets or API keys

## Additional Resources

- Quarkus guides: https://quarkus.io/guides/
- GraalVM native image: https://www.graalvm.org/latest/reference-manual/native-image/
- SmallRye Mutiny: https://smallrye.io/smallrye-mutiny/
