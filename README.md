# IDE Sidecar 

![Release](release.svg) 

👋 Welcome to **IDE Sidecar**, the sidecar application used by
[Confluent for VS Code](https://github.com/confluentinc/vscode).

💡 IDE Sidecar exposes REST and GraphQL APIs.
It is a Java 21 project that uses [Quarkus](https://quarkus.io), [GraalVM](https://www.graalvm.org/), and
Maven. We distribute IDE Sidecar as a native executable, which - compared to traditional
JIT-compiled Java projects - offers a shorter startup time, a smaller container image size,
and a smaller memory footprint.

## Prerequisites for development

These tools must be installed on your workstation.

* [SDKMAN!](https://sdkman.io/)
* [GraalVM CE version 21](https://www.graalvm.org/)
* [Docker](https://www.docker.com/get-started)

Most of these tools can be found in [`brew`](https://brew.sh/) or your favorite package manager.

## Make commands

The following table documents our custom `make` commands. We manage them in the
[Makefile](./Makefile) at the root directory of this repository.
You may find the following `make` commands useful during development:

| Make command       | Description                                   |
|--------------------|-----------------------------------------------|
| `make quarkus-dev`  | Runs the application in the [Quarkus dev mode](https://quarkus.io/guides/getting-started#development-mode).                                                                                                                                                         |
| `make quarkus-test` | Runs the [continuous testing mode of Quarkus](https://quarkus.io/guides/continuous-testing#continuous-testing-without-dev-mode).                                                                                                                                    |
| `make build`       | Build, compile, package JARs, and run tests.  |
| `make clean`       | Clean the project and remove temporary files. |
| `make test`        | Run the unit tests of the project.            |
| `make test-native` | Run the tests against the native executable.  |
| `make mvn-package-native`                | Runs the unit and integration tests and creates a native executable for the project.                                                                                                                                |
| `make mvn-package-native-no-tests`       | Creates a native executable for the project without running the tests.                                                                                                                                              |

## Contributing

All contributions are welcome! Please refer to the [Contribution Guidelines](CONTRIBUTING.md) for more information
on how to contribute to this project.

## Release process

To release a new version of IDE Sidecar, you need to raise a PR against the `main` branch,
get it reviewed, and then merge it. Every new commit (except those with `[ci skip]` in the
description) will cause Semaphore CI/CD to build the `main` branch and create a GitHub release. For successful builds,
Semaphore will create native executables for all supported operating systems and platforms via
[promotions](https://github.com/confluentinc/ide-sidecar/blob/main/.semaphore/semaphore.yml#L78).
Semaphore CI uploads the native executables as GitHub release assets, so that the 
[confluentinc/vscode](https://github.com/confluentinc/vscode) CI job can pull these executables.

At the moment, we build native executables for the following operating systems and platforms:

* Linux (AMD64)
* Linux (ARM64)
* macOS (AMD64)
* macOS (ARM64)

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.txt](./LICENSE.txt) for the full license text.

The LICENSE.txt and NOTICE.txt covers the source code distributions of this project. 
The LICENSE.txt and NOTICE-binary.txt covers the binary distribution (native executable) of this project. 
The THIRD_PARTY_NOTICES.txt file contains the list of third-party software that is included in the 
binary distribution of this project, along with the full text of applicable licenses.
