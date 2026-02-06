# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

IDE Sidecar is a **Java 21 Quarkus application** that serves as the backend for Confluent's VS Code extension. It exposes REST and GraphQL APIs for managing connections to Kafka clusters (CCloud, Local, Direct), consuming/producing messages, and proxying requests. Compiled to a **native executable using GraalVM** for fast startup and low memory footprint.

## Build & Test Commands

Prerequisites: GraalVM CE JDK 21 via SDKMAN (`sdk env install`), Docker.

| Command                                      | Purpose                                                      |
| -------------------------------------------- | ------------------------------------------------------------ |
| `make quarkus-dev`                           | Dev mode with hot reload (port 26636)                        |
| `make build`                                 | Compile, package JARs (skips tests)                          |
| `make test`                                  | Run unit + integration tests (JVM mode)                      |
| `make test TEST=<fully.qualified.ClassName>` | Run a single test class                                      |
| `make test-native`                           | Build native executable and run integration tests against it |
| `make clean`                                 | Clean build artifacts                                        |
| `make mvn-generate-sidecar-openapi-spec`     | Regenerate OpenAPI spec (commit if changed)                  |

Dev mode URLs: App `http://localhost:26636`, Swagger UI `/swagger-ui`, GraphQL UI `/q/graphql-ui`, Dev UI `/q/dev-ui`.

## Code Style

Google Java Style Guide enforced by Checkstyle. 2-space indents, 100-char line limit (see `.editorconfig`). SpotBugs runs at build time. Pre-commit hooks scan for secrets via `gitleaks`.

## Architecture

### Key Directories

- `src/main/java/io/confluent/idesidecar/restapi/resources/` — REST endpoints (`*Resource.java`)
- `src/main/java/io/confluent/idesidecar/restapi/resources/graph/` — GraphQL resolvers (`*QueryResource.java`)
- `src/main/java/io/confluent/idesidecar/websocket/` — WebSocket endpoints and proxy handlers
- `src/main/java/io/confluent/idesidecar/restapi/proxy/` — Processor chain implementations
- `src/main/java/io/confluent/idesidecar/restapi/models/` — DTOs and data models
- `src/main/java/io/confluent/idesidecar/restapi/connections/` — Connection state management
- `src/main/java/io/confluent/idesidecar/restapi/application/` — CDI bean producers (`*BeanProducers.java`)
- `src/main/java/io/confluent/idesidecar/restapi/messageviewer/strategy/` — Kafka consume strategies

### Processor Chain Pattern (Chain of Responsibility)

Request handling uses chained processors via `Processor.chain(...)`:

```java
Processor.chain(
    new ConnectionProcessor<>(connectionManager),
    new KafkaClusterInfoProcessor<>(clusterCache),
    new ConsumeStrategyProcessor(nativeStrategy, ccloudStrategy),
    new EmptyProcessor<>()
)
```

Each processor receives a context object (`ProxyContext`, `ClusterProxyContext`, `KafkaRestProxyContext`), performs its operation, and returns modified context for the next processor. Fail fast via `ProcessorFailedException`. Processors are wired in `*BeanProducers` classes.

### Connection State Management

`ConnectionStateManager` is the single source of truth for connection lifecycle. Uses CDI events with qualifiers like `@Lifecycle.Connected` and `@ServiceKind.CCloud` for the observer pattern.

### Reactive Programming

REST endpoints return `Uni<Response>` (SmallRye Mutiny) for non-blocking I/O. Vert.x used for HTTP clients.

### CDI / Dependency Injection

`@ApplicationScoped` singletons, `@Inject` for field injection, `@Named("beanName")` for named beans. Bean factories live in `*BeanProducers` classes using `@Produces`.

## Testing

- **Unit tests** (`*Test.java`): Use `@QuarkusTest`, mock external dependencies
- **Integration tests** (`*IT.java`): Tagged with `@Tag("io.confluent.common.utils.IntegrationTest")`, use `@ConnectWireMock` for HTTP mocking, run against testcontainers
- **Test port**: 26637 (to avoid conflict with dev port 26636)
- **Auth bypass in tests**: `@TestProfile(NoAccessFilterProfile.class)`
- **Test helpers**: `SidecarClient` in `src/test/java/.../util/` for API calls and GraphQL queries
- **Mock responses**: `src/test/resources/*-mock-responses/` for WireMock fixtures

## Native Executable Considerations

- Use `@RegisterForReflection` on classes accessed via reflection (DTOs, record classes)
- Test with `make test-native` before merging
- GraalVM substitutions for native libs in `src/main/java/.../application/*NativeLoader.java`
- Native build config under `native` Maven profile in `pom.xml`

## Code Generation

- **OpenAPI**: Auto-generated from JAX-RS annotations to `src/generated/resources/openapi.{yaml,json}`. Regenerate with `make mvn-generate-sidecar-openapi-spec` and commit changes.
- **GraphQL schema**: Auto-generated from `@GraphQLApi` resources to `src/generated/resources/schema.graphql`
- **Kafka REST models**: Generated from `kafka-rest.openapi.yaml` into `src/main/java/.../kafkarest/model/`

## Configuration

All config in `src/main/resources/application.yml` with profile overrides:

- `%dev`: Auth disabled, JSON logging off
- `%test`: WireMock enabled, port 26637
- Default (prod): JSON logging on, runs from native executable

Key config namespaces: `ide-sidecar.connections.ccloud.*`, `ide-sidecar.connections.confluent-local.*`, `ide-sidecar.connections.direct.*`, `ide-sidecar.admin-client-configs.*`.

## Dev Authentication

```bash
export DTX_ACCESS_TOKEN=$(curl -s http://localhost:26636/gateway/v1/handshake | jq -r .auth_secret)
curl -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" http://localhost:26636/gateway/v1/connections
```

## PR Guidelines

- Follow Google Java Style Guide
- Catch blocks must never swallow exceptions — always log at ERROR level
- Include Javadocs for code changes; never delete existing Javadocs unless significantly wrong
- Test against native executable before merging
- Regenerate and commit OpenAPI specs if API surface changes
- Do NOT include `[ci skip]` in commit messages unless intentionally skipping CI
