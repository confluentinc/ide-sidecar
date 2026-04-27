---
name: graalvm-docs
description:
  Use when the user asks about GraalVM native image compilation, reflection registration, native
  build failures, or native executable testing. Triggers on questions like "@RegisterForReflection",
  "native image build error", "reflection-config.json", "native library loading", "GraalVM
  substitution", "--initialize-at-run-time", "native executable test", "native build args",
  "ClassNotFoundException in native", or "make test-native".
allowed-tools:
  - Read
  - Bash
  - Glob
  - Grep
  - WebFetch
  - WebSearch
argument-hint: "[topic or error message]"
---

# GraalVM Native Image Documentation Lookup

Look up GraalVM native image documentation via WebFetch and cross-reference with project-specific
native compilation patterns in the ide-sidecar codebase.

## Determine the Project's JDK Version

The project uses GraalVM CE JDK installed via SDKMAN. Check the current version:

```bash
grep -i 'java\|jdk' .sdkmanrc 2>/dev/null; grep 'maven.compiler.release\|java.version' pom.xml | head -3
```

Extract the major JDK version (e.g., `21`).

## Documentation URL Map

### GraalVM Reference

GraalVM docs support version-specific URLs by JDK version. Always use the project's JDK version:

- **Version-specific (preferred)**: `https://www.graalvm.org/jdk{version}/reference-manual/native-image`
- **Latest (fallback)**: `https://www.graalvm.org/latest/reference-manual/native-image`

For example, if the project uses JDK 21, use `https://www.graalvm.org/jdk21/reference-manual/native-image`.

| Topic               | Path                            | Use For                                                                                 |
| ------------------- | ------------------------------- | --------------------------------------------------------------------------------------- |
| Overview            | `/`                             | General concepts, how native image works                                                |
| Reflection          | `/dynamic-features/Reflection/` | Reflection configuration, `reflect-config.json` format                                  |
| JNI                 | `/dynamic-features/JNI/`        | JNI configuration for native libraries                                                  |
| Resources           | `/dynamic-features/Resources/`  | Including files/resources in the native executable                                      |
| Build Configuration | `/overview/BuildConfiguration/` | Build-time args, `--initialize-at-run-time`, `--report-unsupported-elements-at-runtime` |
| Compatibility       | `/metadata/Compatibility/`      | Unsupported features, limitations                                                       |

### Quarkus Native Guides

Quarkus guides also support versioned URLs (see `quarkus-docs` skill for version lookup).

- **Version-specific (preferred)**: `https://quarkus.io/version/{major.minor}/guides`
- **Latest (fallback)**: `https://quarkus.io/guides`

| Topic           | Path                                | Use For                                                    |
| --------------- | ----------------------------------- | ---------------------------------------------------------- |
| Building Native | `/building-native-image`            | Quarkus-specific native image building                     |
| Native Tips     | `/writing-native-applications-tips` | `@RegisterForReflection`, troubleshooting, common patterns |

## Navigation Strategy

1. **Determine the project's JDK version** from `.sdkmanrc` or `pom.xml` (see above)
2. **Match the user's question to one or more topics** from the tables above
3. **Check project-specific files first** — see Project-Specific Locations below
4. **Fetch the relevant page(s)** using version-specific URLs — use a targeted prompt to extract
   only the relevant section
5. **If a question spans topics** (e.g., "reflection for a JNI class"), fetch both pages
6. **For questions not clearly in the table**, use WebSearch:
   `site:graalvm.org native-image <topic>` or `site:quarkus.io/guides/ native <topic>`

### Topic Selection Examples

| User asks about…                            | Check Project Files              | Fetch Docs               |
| ------------------------------------------- | -------------------------------- | ------------------------ |
| "ClassNotFoundException in native"          | `ReflectionConfiguration.java`   | Reflection + Native Tips |
| "how to register a class for reflection"    | `ReflectionConfiguration.java`   | Native Tips              |
| "UnsatisfiedLinkError with compression lib" | `*NativeLoader.java`             | JNI                      |
| "ExceptionInInitializerError during build"  | `application.yml` native section | Build Configuration      |
| "resource not found in native executable"   | `application.yml` native section | Resources                |
| "is dynamic class loading supported?"       | —                                | Compatibility            |
| "make test-native failing"                  | Test logs, `application.yml`     | Building Native          |

## Project-Specific Locations

Rather than duplicating codebase details here, **search these key locations** to understand the
current state of the project's native configuration:

### Reflection Registration

- **Project classes**: Search for `@RegisterForReflection` across `src/main/java/` to see which
  classes are annotated directly
- **Third-party classes**: Read `src/main/java/.../application/ReflectionConfiguration.java` — this
  is the centralized registration point for external library classes that need reflection
- **Convention**: annotate project classes directly; add third-party classes to
  `ReflectionConfiguration.java`

### Native Library Loading

- **Loader pattern**: Read `src/main/java/.../application/SnappyNativeLoader.java` and
  `ZstdNativeLoader.java` — these demonstrate the pattern for extracting native libs at startup
- **Shared utility**: Search for `NativeLibraryUtil` to find the extraction helper

### Native Image Configuration Files

- **Config directory**: `src/main/resources/META-INF/native-image/` — organized by library
  (e.g., `com.github.luben/zstd-jni/`, `com.google.protobuf/`). List this directory to see
  current configs.
- **Contents**: `reflect-config.json`, `jni-config.json`, `resource-config.json` per library

### Build Arguments

- **Native build config**: Read the `quarkus.native` section in
  `src/main/resources/application.yml` — contains `additional-build-args` (including
  `--initialize-at-run-time` class list), `march`, and `resources.includes`
- **To find current init-at-run-time classes**: grep for `initialize-at-run-time` in
  `application.yml`

## Common Error Patterns

When a user reports a native-mode error, check this table for likely causes, then search the
project files to confirm:

| Error at Runtime                            | Likely Cause                                    | Where to Fix                                                    |
| ------------------------------------------- | ----------------------------------------------- | --------------------------------------------------------------- |
| `ClassNotFoundException`                    | Missing reflection registration                 | `@RegisterForReflection` or `ReflectionConfiguration.java`      |
| `NoSuchMethodException`                     | Method not registered for reflection            | `reflect-config.json` or `@RegisterForReflection(methods=true)` |
| `MissingResourceException` / file not found | Resource not included in native executable      | `quarkus.native.resources.includes` in `application.yml`        |
| JNI `UnsatisfiedLinkError`                  | Native library not loaded or JNI not configured | `*NativeLoader.java` pattern + `jni-config.json`                |
| `ExceptionInInitializerError` during build  | Static initializer needs runtime state          | `--initialize-at-run-time` in `application.yml`                 |
| `UnsupportedFeatureException`               | Feature not available in native mode            | Refactor; check GraalVM Compatibility docs                      |

## Output Format

- **Diagnosis first**: explain what's happening and why it occurs in native mode
- **Project-specific solution**: show exactly where and how to apply the fix in this codebase, with
  file paths and existing patterns
- **Official reference**: relevant excerpt from GraalVM/Quarkus docs
- **Verification**: how to verify the fix (`make test-native` or specific test commands)
- Link back to the doc URL so the user can read further

## Tips

- Always check `ReflectionConfiguration.java` before adding a new `reflect-config.json` file — the
  centralized approach is preferred for third-party classes in this project
- When adding a new third-party library dependency, proactively check if it uses reflection, JNI, or
  resources that need native image configuration
- Native build failures are often only caught by `make test-native`, not `make test` — always
  recommend running native tests when changes touch serialization, reflection, or new dependencies
- If a doc URL returns sparse content or a 404, use WebSearch as fallback to find the current page
- For Quarkus-specific native patterns (like `@RegisterForReflection`), prefer the Quarkus Native
  Tips guide — it's more practical than the GraalVM reference docs
