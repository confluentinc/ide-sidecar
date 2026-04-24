---
name: quarkus-docs
description:
  Use when the user asks about Quarkus framework APIs, CDI dependency injection, SmallRye Mutiny
  reactive programming, JAX-RS REST endpoints, Quarkus testing, or application configuration.
  Triggers on questions like "how does @Inject work", "Uni vs Multi", "Quarkus test profile",
  "application.yml config", "@ApplicationScoped vs @Singleton", "@Produces bean", "@Observes event",
  "CDI observer pattern", "Mutiny transform", "Vert.x HTTP client", or "Quarkus guide for X".
allowed-tools:
  - Read
  - Bash
  - Glob
  - Grep
  - WebFetch
  - WebSearch
argument-hint: "[topic]"
---

# Quarkus Documentation Lookup

Look up Quarkus framework documentation via WebFetch and cross-reference with project-specific
patterns in the ide-sidecar codebase.

## Determine the Project's Quarkus Version

Read the project's `pom.xml` to find the Quarkus platform version:

```bash
grep 'quarkus.platform.version' pom.xml | head -1
```

The version will be something like `3.27.0` — extract the major.minor (e.g., `3.27`).

## Documentation URL Map

Quarkus guides support version-specific URLs. Always use the project's version:

- **Version-specific (preferred)**: `https://quarkus.io/version/{major.minor}/guides/{guide}`
- **Latest (fallback)**: `https://quarkus.io/guides/{guide}`

For example, if the project uses Quarkus 3.27.0, use `https://quarkus.io/version/3.27/guides/cdi`.

| Topic             | Path                       | Use For                                                                                     |
| ----------------- | -------------------------- | ------------------------------------------------------------------------------------------- |
| CDI Introduction  | `/cdi`                     | Bean discovery, basic injection, scopes                                                     |
| CDI Reference     | `/cdi-reference`           | `@Inject`, `@ApplicationScoped`, `@Produces`, `@Named`, qualifiers, interceptors, observers |
| Mutiny Primer     | `/mutiny-primer`           | `Uni<T>`, `Multi<T>` basics, operators, error handling                                      |
| RESTEasy Reactive | `/resteasy-reactive`       | JAX-RS endpoints returning `Uni<Response>`, `@Path`, `@GET`/`@POST`                         |
| Testing           | `/getting-started-testing` | `@QuarkusTest`, test profiles, injection in tests                                           |
| Configuration     | `/config-reference`        | `application.yml`, profile overrides (`%dev`, `%test`), `@ConfigProperty`                   |
| Vert.x            | `/vertx`                   | Vert.x integration, `WebClient`, event bus                                                  |
| Scheduler         | `/scheduler-reference`     | `@Scheduled` for periodic tasks                                                             |
| SmallRye Health   | `/smallrye-health`         | Health check endpoints                                                                      |
| All Guides Index  | `/`                        | Finding guides not listed above                                                             |

## Navigation Strategy

1. **Determine the project's Quarkus version** from `pom.xml` (see above)
2. **Match the user's question to one or more topics** from the table above
3. **Fetch the relevant page(s)** using the version-specific URL
   (`https://quarkus.io/version/{major.minor}/guides{path}`) — use a targeted prompt to extract
   only the relevant section (pages can be very large)
4. **If a question spans topics** (e.g., "how to inject a bean in a test"), fetch both pages
5. **Check project conventions** — search the local codebase for existing usage of the pattern
6. **For questions not clearly in the table**, use WebSearch: `site:quarkus.io/guides/ <topic>`

### Topic Selection Examples

| User asks about…                             | Fetch                   |
| -------------------------------------------- | ----------------------- |
| "how does @Inject work?"                     | CDI Introduction        |
| "@Produces vs @Named"                        | CDI Reference           |
| "Uni vs Multi, when to use which?"           | Mutiny Primer           |
| "how to return async response from endpoint" | RESTEasy Reactive       |
| "QuarkusTest not injecting my bean"          | Testing + CDI Reference |
| "how do profile overrides work?"             | Configuration           |
| "Vert.x WebClient usage"                     | Vert.x                  |

## Project-Specific Conventions

Rather than duplicating codebase details here, **search these key locations** to understand how the
project uses Quarkus patterns:

### CDI Bean Wiring

- **Bean producer classes**: `src/main/java/.../application/*BeanProducers.java` — this project
  wires processor chains using `@Produces` + `@Singleton` + `@Named` in dedicated producer classes.
  Read these files for the canonical pattern.
- **CDI event qualifiers**: `src/main/java/.../events/` — custom qualifiers for connection lifecycle
  (`@Lifecycle.*`, `@ServiceKind.*`, `@ClusterKind.*`). Read to understand the observer pattern.

### Reactive Patterns

- **Mutiny helpers**: `src/main/java/.../util/MutinyUtil.java` — project-specific utilities for
  common Mutiny patterns. Check here before writing new reactive code.
- **Processor chain**: `src/main/java/.../processors/Processor.java` — abstract Chain of
  Responsibility base class; chains are wired in the `*BeanProducers` classes.

### Testing

- **Test helper**: `src/test/java/.../util/SidecarClient.java` — reusable client for API and
  GraphQL calls in tests.
- **Test profiles and mock responses**: Search for `@TestProfile` and `@ConnectWireMock` usage in
  `src/test/java/` to see how existing tests are structured.
- **Mock fixtures**: `src/test/resources/*-mock-responses/`

### Configuration

- **Application config**: `src/main/resources/application.yml` — look for `%dev` and `%test` profile
  overrides and custom `ide-sidecar.*` namespaces.

## Output Format

- Present official docs excerpt alongside project-specific examples from the codebase
- Note gotchas and common mistakes specific to this project's Quarkus usage
- Link back to the guide URL so the user can read further
- When the project already has an established pattern, show the existing code rather than inventing
  a new approach

## Tips

- Quarkus guide pages can be very large — always use a targeted WebFetch prompt to extract the
  relevant section rather than processing the entire page
- When the user asks about reactive patterns, check `MutinyUtil.java` first — it likely has a
  helper for the common case
- The `%test` profile in `application.yml` overrides many defaults — always check it when debugging
  test-specific behavior
- CDI `@Produces` methods in this project follow a consistent pattern in `*BeanProducers.java` —
  look there first for examples of wiring new beans
- If a guide URL returns sparse content or a 404, use WebSearch as fallback to find the current URL
