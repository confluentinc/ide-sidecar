# IDE Sidecar 

[![Build Status](https://semaphore.ci.confluent.io/badges/ide-sidecar/branches/main.svg?style=shields&key=201261ed-331f-447a-a58a-90ad092b0d56)](https://semaphore.ci.confluent.io/projects/ide-sidecar) ![Release](release.svg) 

[![Quality Gate Status](https://sonarqube.dp.confluent.io/api/project_badges/measure?project=ide-sidecar&metric=alert_status&token=sqb_030bd6909eb0847e7effeeeaf2c9fade23e5afb9)](https://sonarqube.dp.confluent.io/dashboard?id=ide-sidecar) [![Coverage](https://sonarqube.dp.confluent.io/api/project_badges/measure?project=ide-sidecar&metric=coverage&token=sqb_030bd6909eb0847e7effeeeaf2c9fade23e5afb9)](https://sonarqube.dp.confluent.io/dashboard?id=ide-sidecar) [![Technical Debt](https://sonarqube.dp.confluent.io/api/project_badges/measure?project=ide-sidecar&metric=sqale_index&token=sqb_030bd6909eb0847e7effeeeaf2c9fade23e5afb9)](https://sonarqube.dp.confluent.io/dashboard?id=ide-sidecar) [![Security Hotspots](https://sonarqube.dp.confluent.io/api/project_badges/measure?project=ide-sidecar&metric=security_hotspots&token=sqb_030bd6909eb0847e7effeeeaf2c9fade23e5afb9)](https://sonarqube.dp.confluent.io/dashboard?id=ide-sidecar) [![Lines of Code](https://sonarqube.dp.confluent.io/api/project_badges/measure?project=ide-sidecar&metric=ncloc&token=sqb_030bd6909eb0847e7effeeeaf2c9fade23e5afb9)](https://sonarqube.dp.confluent.io/dashboard?id=ide-sidecar)

👋 Welcome to an early prototype of **IDE Sidecar**, the sidecar application used by
the [DTX VS Code Extension](https://github.com/confluentinc/vscode-extension).

💡 IDE Sidecar exposes a REST API.
It is a Java 21 project that uses [Quarkus](https://quarkus.io), [GraalVM](https://www.graalvm.org/), and
Maven. We intend to distribute IDE Sidecar as a native executable, which - compared to traditional
JIT-compiled Java projects - offers a shorter startup time, a smaller container image size,
and a smaller memory footprint.



## Prerequisites

These tools must be installed on your workstation.

* [SDKMAN!](https://sdkman.io/)
* [GraalVM CE version 21](https://www.graalvm.org/)
* [Docker](https://www.docker.com/get-started)

Most of these tools can be found in [`brew`](https://brew.sh/) or your favorite package manager.

Before running the below Maven commands, please make sure that you are authenticated with Confluent's Maven
repository by executing:

```shell script
maven-login
```

## Make commands

The following table documents our custom `make` commands. We manage them in the
[Makefile](./Makefile) at the root directory of this repository. When introducing a new custom
`make` command to the [Makefile](./Makefile), make sure to not change the file section managed
by ServiceBot. Otherwise they will be overwritten by the next run of ServiceBot.

| Make command                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `make mvn-package-native`                | Creates a native executable for the project.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `make mvn-package-native-no-tests`       | Creates a native executable for the project without running the tests.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `make zip-templates`                     | Creates a zip archive out of the directories [src/main/resources/static](./src/main/resources/static) and [src/test/resources/static](./src/test/resources/static). This command is called by our build and test suite; manual invocations are probably not useful.                                                                                                                                                                                                                                                                                                                                                                          |
| `make ci-sign-notarize-macos-executable` | [Semaphore CI Only] Used for signing and notarizing macOS executables.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | 

You may find the following `make` commands useful during development:

| Make command       | Description                                   |
|--------------------|-----------------------------------------------|
| `make build`       | Build, compile, package JARs, and run tests.  |
| `make clean`       | Clean the project and remove temporary files. |
| `make test`        | Run the unit tests of the project.            |
| `make test-native` | Run the tests against the native executable.  |


## Using Sidecar

This section shows how to use `curl` to interact with a running Sidecar.

### Handshake

Sidecar uses authentication to reduce the likelihood that unauthorized clients requires clients provide a shared authorization token. The sidecar returns an authentication token from the first invocation of the `/gateway/v1/handshake` route.

After starting the Sidecar executable, get the token and store in an environment variable:
```shell
export DTX_ACCESS_TOKEN=$(curl -s -H "Content-Type:application/json" http://localhost:26636/gateway/v1/handshake | jq -r .auth_secret)
```
and then verify it is set:
```shell
echo $DTX_ACCESS_TOKEN
```

### List connections
```shell
curl -s -H "Content-Type:application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" http://localhost:26636/gateway/v1/connections | jq -r .
```
After starting, there will be no connections
```json
{
  "api_version": "gateway/v1",
  "kind": "ConnectionsList",
  "metadata": null,
  "data": []
}
```

### Create a new CCloud connection and sign in

Create a new CCloud connection with `c1` as the ID (or use a different ID):
```shell
curl -s -X POST -d'{"id": "c1", "name": "DTX", "type": "CCLOUD"}' -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -H "Content-Type:application/json" http://localhost:26636/gateway/v1/connections | jq -r .
```
This should return the new connection details, such as the following (the `sign_in_uri` value has been truncated):
```json
{
  "api_version": "gateway/v1",
  "kind": "Connection",
  "id": "c1",
  "metadata": {
    "self": "http://localhost:26636/gateway/v1/connections/c1",
    "resource_name": null,
    "sign_in_uri": "https://login-stag.confluent-dev.io/..."
  },
  "spec": {
    "id": "c1",
    "name": "DTX",
    "type": "CCLOUD"
  },
  "status": {
    "authentication": {
      "status": "NO_TOKEN"
    }
  }
}
```
Open the link shown in the `sign_in_uri` in your browser. Hovering your mouse over the link and pressing `Cmd` key may turn it into a clickable link.

Listing connections again:

```shell
curl -s -H "Content-Type:application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" http://localhost:26636/gateway/v1/connections | jq -r .
```
will include the CCloud connection and it's `status.authentication.status` will be `VALID_TOKEN`, signifying the session has been authenticated:

```json
{
  "api_version": "gateway/v1",
  "kind": "ConnectionsList",
  "metadata": null,
  "data": [
    {
      "api_version": "gateway/v1",
      "kind": "Connection",
      "id": "c1",
      "metadata": {
        "self": "http://localhost:26636/gateway/v1/connections/c1",
        "resource_name": null,
        "sign_in_uri": "https://login-stag.confluent-dev.io/..."
      },
      "spec": {
        "id": "c1",
        "name": "DTX",
        "type": "CCLOUD"
      },
      "status": {
        "authentication": {
          "status": "VALID_TOKEN",
          "requires_authentication_at": "2024-06-12T20:37:14.709390Z"
        }
      }
    }
  ]
}
```

### Create a new Confluent Local connection

Create a new Confluent Local connection with `c2` as the ID (or use a different ID):
```shell
curl -s -X POST -d'{"id": "c2", "name": "DTX", "type": "LOCAL"}' -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -H "Content-Type:application/json" http://localhost:26636/gateway/v1/connections | jq -r .
```
This should return the new connection details, such as the following:
```json
{
  "api_version": "gateway/v1",
  "kind": "Connection",
  "id": "c2",
  "metadata": {
    "self": "http://localhost:26636/gateway/v1/connections/c2",
    "resource_name": null
  },
  "spec": {
    "id": "c2",
    "name": "DTX",
    "type": "LOCAL"
  },
  "status": {
    "authentication": {
      "status": "NO_TOKEN"
    }
  }
}
```
You don't need to authenticate with Confluent Local but can start using the connection immediately.

### Updating connection details

The `PUT /gateway/v1/connections/{id}` endpoint allows you to update any of the connection's details (except the `id`) by passing
the full connection object containing any number of updated fields.

For example, to update the name of the `c1` connection to `DTX CCloud`:
```shell
curl -s -X PUT -d'{"id": "c1", "name": "DTX CCloud", "type": "CCLOUD"}' -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -H "Content-Type:application/json" http://localhost:26636/gateway/v1/connections/c1 | jq -r .
```

### Switching between CCloud Organizations

To switch to a different CCloud organization, use the `PUT /gateway/v1/connections/{id}` endpoint to
update the `ccloud_config.organization_id` field. This triggers an authentication refresh allowing you to continue
querying resources in the new organization.

```shell
curl -s -X PUT -d'{"id": "c1", "name": "DTX", "type": "CCLOUD", "ccloud_config": {"organization_id": "updated-org-id"}}' -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -H "Content-Type:application/json" http://localhost:26636/gateway/v1/connections/c1 | jq -r .
```

> [!NOTE]
> By default, when creating a connection the `ccloud_config` field is set to `null` and we rely on the default 
> organization for the user. You may also create the connection with the `ccloud_config.organization_id` field set to the desired
> organization ID. In this case, the user will be authenticated with the specified organization when they go through the 
> sign-in flow. Switching between orgs on an authenticated connection is as simple as updating the `ccloud_config.organization_id` field.

Here's the GraphQL query to list the organizations for a CCloud connection:
```shell
curl -s -X POST "http://localhost:26636/gateway/v1/graphql" -H "Content-Type: application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -d '{
  "query":"query orgs {
  ccloudConnectionById (id: \"c1\") {
    id
    name
    type
    organizations {
      id
      name
      current
    }
  }
}
"
}' | jq -r .
```

Example response is as follows. Notice that the `current` field indicates the current organization.

```json
{
  "data": {
    "ccloudConnectionById": {
      "id": "c1",
      "name": "DTX",
      "type": "CCLOUD",
      "organizations": [
        {
          "id": "a505f57e-658a-40bc-8a2e-3e88883bc4db",
          "name": "my-main-org",
          "current": true
        },
        {
          "id": "c909fa11-c942-44df-be10-63ba7d5baf85",
          "name": "my-other-org",
          "current": false
        },
        {
          "id": "83ae6822-8cda-4522-8b71-272d7be149f1",
          "name": "my-third-org",
          "current": false
        }
      ]
    }
  }
}
```

### Issue GraphQL query
The following `curl` command shows how to issue GraphQL queries:
```shell
curl -s -X POST "http://localhost:26636/gateway/v1/graphql" -H "Content-Type: application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -d '{
  "query":"<QUERY>"
}' | jq -r .
```
where `<QUERY>` should be replaced with the full GraphQL query expression, which typically starts with `query`.

For example, this GraphQL query lists the ID and name of the CCloud connections:
```graphql
query ccloudConnections {
  ccloudConnections{
    id
    name
  }
}
```
This can be submitted with this `curl` command:
```shell
curl -s -X POST "http://localhost:26636/gateway/v1/graphql" -H "Content-Type: application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -d '{
  "query":"query ccloudConnections {
  ccloudConnections{
    id
    name
  }
}
"
}' | jq -r .
```

Here is a GraphQL query that returns CCloud connections including all nested resources:
```graphql
query ccloudConnections {
  ccloudConnections{
    id
    name
    type
    organizations {
      id
      name
      current
    }
    environments {
      id
      name
      organization {
        id
        name
      }
      kafkaClusters {
        id
        name
        provider
        region
        bootstrapServers
        uri
        organization {
          id
          name
        }
        environment {
          id
          name
        }
      }
      schemaRegistry {
        provider
        region
        uri
        organization {
          id
          name
        }
        environment {
          id
          name
        }
      }
    }
  }
}
```

Here is a GraphQL query that returns Confluent Local connections including their Kafka clusters:

```graphql
query localConnections {
  localConnections{
    id
    name
    type
    kafkaCluster {
      id
      name
      bootstrapServers
      uri
    }
  }
}
```

### View the GraphQL schema
```shell
curl -s -H "Content-Type:application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" http://localhost:26636/gateway/v1/graphql/schema.graphql
```

### Invoking the Kafka and Schema Registry Proxy APIs

The Kafka and Schema Registry Proxy APIs are available at the following endpoints:

- Kafka Proxy:
  - `http://localhost:26636/kafka/v3/clusters/{clusterId}*` (e.g., `http://localhost:26636/kafka/v3/clusters/lkc-abcd123/topics`)
  - **Required headers**:
    - `Authorization: Bearer ${DTX_ACCESS_TOKEN}`
    - `x-connection-id: <connection-id>`
  - **Optional headers**:
    - `x-cluster-id: <kafka-cluster-id>` (If provided, it must match the `clusterId` path parameter.)
- Schema Registry Proxy:
  - Schemas (v1) API at `http://localhost:26636/schemas*` (e.g., `http://localhost:26636/schemaregistry/v1/schemas/ids/schema-1`)
  - Subjects (v1) API at `http://localhost:26636/subjects*` (e.g., `http://localhost:26636/schemaregistry/v1/subjects/subject-id/versions`)
  - **Required headers**:
    - `Authorization: Bearer ${DTX_ACCESS_TOKEN}`
    - `x-connection-id: <connection-id>`
    - `x-cluster-id: <sr-cluster-id>` (Notice the `x-cluster-id` header is only required for the Schema Registry Proxy API.)


> [!IMPORTANT] 
> Pre-requisite conditions for invoking the Kafka and Schema Registry Proxy APIs
> 1. A connection must be created and authenticated.
> 1. A cluster listing query must have been issued (see example below) for the sidecar to cache the cluster information for a given connection. (This is necessary for the Kafka and Schema Registry Proxy APIs to work, otherwise, the sidecar will return a `500` error with the message `Failed to find cluster id=... in the cache`.)


#### List Kafka topics using the Kafka Proxy API

First, we list the Kafka clusters using GraphQL:

```shell
curl -s -X POST "http://localhost:26636/gateway/v1/graphql" -H "Content-Type: application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -d '{
  "query":"query findCCloudKafkaClusters { 
      findCCloudKafkaClusters(connectionId: \"c1\") {
        id
        name
        provider
        region
        bootstrapServers
        uri
      }
    }"
}' | jq -r .
```

Assuming the Kafka cluster ID is `lkc-abcd123`, we can list the topics:

```shell
curl -s \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  http://localhost:26636/kafka/v3/clusters/lkc-abcd123/topics | jq -r .
```

#### Create a Kafka topic using the Kafka Proxy API

Assuming you've already listed the Kafka clusters and have the cluster ID `lkc-abcd123`, you can create a Kafka topic with the following command:

```shell
curl -s -X POST \
  -H "Content-Type:application/json" \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  -d '{"topic_name": "foo"}' \
  http://localhost:26636/kafka/v3/clusters/lkc-abcd123/topics | jq -r .
```

#### List Schemas using the Schema Registry Proxy API

First, we list the Schema Registry clusters using GraphQL:

```shell
curl -s -X POST "http://localhost:26636/gateway/v1/graphql" -H "Content-Type: application/json" -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" -d '{
  "query":"query ccloudConnections {
  ccloudConnections{
    id
    environments {
      id
      schemaRegistry {
        id
        provider
        region
        uri
        organization {
          id
          name
        }
        environment {
          id
          name
        }
      }
    }
  }
 }"
}' | jq -r .
```

Assuming the Schema Registry cluster ID is `lsrc-defg456`, we can list the schemas with the following command:

```shell
curl -s \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  -H "x-cluster-id: lsrc-defg456" \
  http://localhost:26636/schemas?subjectPrefix=:*:&latestOnly=true | jq -r .
```

## Contributing

### Setup IntelliJ code style and Checkstyle

This project uses the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).
We use Checkstyle to enforce this style guide. Follow the steps below to configure IntelliJ to report Checkstyle violations
and
format the code according to the Google Java Style Guide:

1. Install the Checkstyle-IDEA plugin in IntelliJ and point it to the `checkstyle.xml` file at this
   URL: https://raw.githubusercontent.com/confluentinc/common/master/build-tools/src/main/resources/checkstyle/checkstyle.xml.
   For complete instructions, follow
   this [Wiki](https://confluentinc.atlassian.net/wiki/spaces/Engineering/pages/1085800896/Development+Process#DevelopmentProcess-CodingStyle).
2. Set GoogleStyle as the default code style in IntelliJ. Go
   to `Settings` -> `Editor` -> `Code Style` -> `Java` -> `Import Scheme` -> `IntelliJ Idea Code Style XML` and select
   the `intellij-java-google-style.xml` file. (Download from this
   URL: https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml)

## Release process

To release a new version of IDE Sidecar, you need to raise a PR against the `main` branch,
get it reviewed, and then merge it. Every new commit (except those with `[ci skip]` in the
description) will cause Semaphore CI/CD to build the `main` branch. For successful builds,
Semaphore will create native executables for all supported operating systems and platforms via
[promotions](https://github.com/confluentinc/ide-sidecar/blob/main/.semaphore/semaphore.yml#L78).
Semaphore stores the native executables on S3, so that the extension's CI job can pull these
executables.

At the moment, we build native executables for the following operating systems and platforms:

* macOS (AMD64)
* macOS (ARM64)

## FAQs

### Port conflicts

Sidecar's http server is at fixed port 26636. A static
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
./mvnw clean quarkus:dev
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
