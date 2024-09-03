# IDE Sidecar 

![Release](release.svg) 

👋 Welcome to **IDE Sidecar**, the sidecar application used by
the [Confluent Extension for VS Code](https://github.com/confluentinc/vscode).

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

## Troubleshooting

### Port conflicts

Sidecar's HTTP server is at fixed port 26636. A static
port is needed for OAuth negotiation. Therefore, you can only have one such process
running. If you are met with:
```
2024-06-06 10:41:47,189 ERROR [io.qua.run.Application] (main) Port 26636 seems to be in use by another process. Quarkus may already be running or the port is used by another application.
2024-06-06 10:41:47,189 WARN  [io.qua.run.Application] (main) Use 'netstat -anv | grep 26636' to identify the process occupying the port.
```
You can do one better than the suggested pipeline, which does include the server's process
id, but as space delimited field #9. Use awk to cut just that field out, and feed the result
into kill to get rid of the old process:
```shell script
$ kill $(netstat -anv | grep 26636 | awk '{print $9}')
```

Then you should be clear to manually start a new one.

### Build failing due to wrong Java version: "release version 21 not supported"

If you see something like,
```
make quarkus-dev
...
[ERROR] Failed to execute goal io.quarkus.platform:quarkus-maven-plugin:3.10.2:dev (default-cli) on project outpost-scaffolding: Fatal error compiling: error: release version 21 not supported -> [Help 1]
```

Then, it is likely that your Jenv is activated and pointing to a Java version that is not 21. You might have the following
in your `.zshrc` or `.bashrc`:
```shell script
export PATH="$HOME/.jenv/bin:$PATH"
eval "$(jenv init -)"
```

You can deactivate Jenv by commenting out the `eval` line. Restart your shell to make sure you're now using SDKMAN's
GraalVM 21.

To double check, run `sdk env`. You should see:
```shell script
$ sdk env
Using java version 21.0.2-graalce in this shell.
```
