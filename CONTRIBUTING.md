# Welcome to the Confluent ide-sidecar contributing guide

Thanks for your interest in contributing to this project, which is part of the Confluent extension for VS Code!
Our goal for the [Confluent extension for VS Code project](https://github.com/confluentinc/vscode) 
is to help make it very easy for developers to build stream processing applications using Confluent.

Anyone can contribute, and here are some ways to do so:
* [report problems](https://github.com/confluentinc/ide-sidecar/issues)
* reviewing and verifying [pull requests](https://github.com/confluentinc/ide-sidecar/pulls)
* creating [pull requests](https://github.com/confluentinc/ide-sidecar/pulls) with code changes to fix bugs, improve documentation, add/improve tests, and/or implement new features.

This document outlines the basic steps required to work with and contribute to this project.

Use the Table of contents icon in the top left corner of this document to get to a specific section of this guide quickly.

## New contributor guide
To get an overview of the project, read the [README](./README.md) file. Here are some resources to help you get started with open source contributions:

- [Finding ways to contribute to open source on GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/finding-ways-to-contribute-to-open-source-on-github)
- [Set up Git](https://docs.github.com/en/get-started/getting-started-with-git/set-up-git)
- [GitHub flow](https://docs.github.com/en/get-started/using-github/github-flow)
- [Collaborating with pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests)

## Issues

You can [report problems or comment on issues](https://github.com/confluentinc/ide-sidecar/issues) without installing the tools, getting the code, or building the code. All you need is a GitHub account.

### Create a new issue

If you spot a problem with the docs, [search if an issue already exists](https://docs.github.com/en/github/searching-for-information-on-github/searching-on-github/searching-issues-and-pull-requests#search-by-the-title-body-or-comments). If a related issue doesn't exist, you can open a new issue using a relevant [issue form](https://github.com/confluentinc/ide-sidecar/issues/new/choose).

### Solve an issue

Scan through our [existing issues](https://github.com/github/confluentinc/ide-sidecar/issues) to find one that interests you. You can narrow down the search using `labels` as filters. See "[Label reference](https://docs.github.com/en/contributing/collaborating-on-github-docs/label-reference)" for more information. As a general rule, you are welcome to open a PR with a fix unless that issue is already assigned to someone else, or someone else has added a comment that they are working on it.


## Install the tools

If you want to work with this project's codebase and maybe contribute to it, you will need to have some development tools. This project uses the following software that you may already have:

* [Git](https://git-scm.com) -- version 2.40.0 or later
* [Apache Maven](https://maven.apache.org/maven/) -- build tool for Java projects
* [Maven Wrapper](https://maven.apache.org/wrapper/) -- we use this wrapper around Maven that will download the correct version of Maven, if it's different than what you have installed
* [Docker](https://www.docker.com/get-started) -- used to locally run containers of services during integration tests

See the links above for installation instructions on your platform. You can verify the versions you have installed and that they are working.

    git --version

should be at least 2.40.0 or later,

    java --version

should be `21.0.2` or later and include `Oracle GraalVM` in the output, and 

    docker --version

should be `20.10.21` or later.

The project also uses these tools:

* [GraalVM for JDK 21 Community Edition (CE)](https://github.com/graalvm/graalvm-ce-builds/) -- used to compile and package Java 21 source code into JARs or native executables.
* [SDKMAN!](https://sdkman.io/) -- utilities for installing SDKs, including GraalVM
* [pre-commit](https://pre-commit.com/) -- security tooling to prevent checking in secrets

We'll install these after you clone the repository.


## Other services

The project also uses several services:
* [GitHub](https://github.com) -- this project is on GitHub, so to contribute you'll need a GitHub account.
* [Semaphore CI/CD](https://semaphoreci.com/) -- continuous integration and deployment service. You should not need an account.

## General development process

Bugs, feature requests, and suggested changes are tracked through the project's [GitHub issues](https://github.com/confluentinc/ide-sidecar/issues).

All changes are made through [pull requests (PRs)](https://github.com/confluentinc/ide-sidecar/pulls).
Every PR's [Semaphore CI/CD build](https://semaphoreci.com/) must pass and code coverage (reported as comments on the PR) should either improve or not appreciably change. The Confluent team will review PRs and provide feedback; once the changes in the PR are acceptable, the team will merge the PR onto the appropriate branch.

To create a PR, you must create a fork of this repository and set up your machine with the tools needed for development. These steps
are outlined below.

Most development occurs on the `main` branch. Therefore, most PRs will target the `main` branch, and be merged to the `main` branch. We use [semantic versioning](https://semver.org/), so our version numbers are of the form `v.MAJOR.MINOR.PATCH`, such as `v1.2.0`. We will release all major and minor releases from the `main` branch.

If we need to patch a previously-released major or minor release, we will create a `v.MAJOR.MINOR.x` branch (e.g., `v1.2.x`), and we create PRs against this branch for all fixes and changes. When the patch is ready, we'll release the first `v.MAJOR.MINOR.1` patch version (e.g., `v1.2.1`). If we need to make additional fixes, we'll continue to do so against this same branch and release subsequent patch versions (e.g., `v1.2.2`, `v1.2.3`, etc).

This project's releases will be published to https://github.com/confluentinc/ide-sidecar/releases, 
and those releases will be used by the [Confluent extension for VS Code project](https://github.com/confluentinc/vscode).

## Our codebase

This repo mostly follows [Maven's Standard Directory Layout](https://maven.apache.org/guides/introduction/introduction-to-the-standard-directory-layout.html) for single module projects.
Here's the basic file structure:


    ide-sidecar/
    |- src/
    |  |- main/
    |  |  |- generated/          (Directory with files generated from other source code, including OpenAPI files and GraphQL schemas)
    |  |  |- java/               (Directory with Java source files for the server)
    |  |  |- resources/          (Directory with resource files for the server)
    |  |- test/
    |     |- java/               (Directory with Java source files for unit and integration tests)
    |     |- resources/          (Directory with test resource files)
    |- pom.xml                   (The readme file for this repository)
    |- LICENSE.txt               (The license information for this repository)
    |- NOTICES.txt               (Notices and attributions required by libraries that the project depends on; generated with each release and included in distributions)
    |- README.md                 (The readme file for this repository)

The `target` directory is used to house all output of the build, and is created as needed.

There are other top-level directories and files that are not specific to Maven:

    ide-sidecar/
    |- .github/                  (Directory containing workflows, issue templates, pull request templates, and other files
    |- .mvn/                     
    |   |- wrapper/              (Directory with files used by the Maven wrapper)
    |   |- settings.xml          (Project-specific Maven settings.xml file, helps to avoid bringing in non-public libraries or versions)
    |- .semaphore/               (Directory containing files used by Semaphore CI/CD
    |- .versions/                (Directory containing files used by the build)
    |- build-tools/              (Directory containing scripts and tooling used for automation)
    |- mk-files/                 (Directory containing makefile include files)
    |- .gitignore                (File that defines the files and directories that are not be added to this repository)
    |- Makefile                  (The makefile for the project)
    |- mvnw                      (The Maven wrapper command line utility, used in place of `mvn`)
    |- service.yml               (File with the configuration for automated Confluent tooling for managing repositories)
    |- sonar-project.properties  (File with the configuration for code quality automation)
    |...


## Working with the codebase

This section outlines the one-time setup and installation of some tools. It then shows the basics of building and testing the code

### One time setup

#### Fork this repository

Go to [this repository on GitHub](https://github.com/confluentinc/ide-sidecar) and click the "Fork" button near the upper right corner of the page. Complete the form and click the "Create fork" button to create your own https://github.com/YOUR-USERNAME/ide-sidecar repository. This is the repository to which you will upload your proposed changes and create pull requests. See the [GitHub documentation](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) for details.

#### Clone your fork

To work locally on the code, you need to pull the code onto your machine. 
At a terminal, go to the directory in which you want to place a local clone of this repository, and run the following commands to use SSH authentication (recommended):

    git clone git@github.com:YOUR-USERNAME/ide-sidecar.git

or with HTTPS:

    git clone https://github.com/YOUR-USERNAME/ide-sidecar.git

This will create a `ide-sidecar` directory and pull the contents of your forked repository. Change into that directory:

    cd ide-sidecar

#### Sync your repository with ours

If you intend to propose changes to our upstream repository, you should next configure your local repository to be able to pull code from the project's _remote_ repository, called the _upstream_ repository.

Use the following command to see the current remotes for your fork:

    git remote -v

which will output something like:

    origin  git@github.com:YOUR-USERNAME/ide-sidecar.git (fetch)
    origin  git@github.com:YOUR-USERNAME/ide-sidecar.git (push)

or if you used HTTPS:

    origin  https://github.com/YOUR-USERNAME/ide-sidecar.git (fetch)
    origin  https://github.com/YOUR-USERNAME/ide-sidecar.git (push)

Then run the following command to add the project's repository as a remote called `upstream`:

    git remote add upstream git@github.com:confluentinc/ide-sidecar.git

or if you've used HTTPS:

    git remote add upstream https://github.com/confluentinc/ide-sidecar.git

To verify the new upstream repository you have specified for your fork, run this command again:

    git remote -v

You should see the URL for your fork as `origin`, and the URL for the project's upstream repository as `upstream`. If you used SSH, this will look something like:

    origin  git@github.com:YOUR-USERNAME/ide-sidecar.git (fetch)
    origin  git@github.com:YOUR-USERNAME/ide-sidecar.git (push)
    upstream  git@github.com:confluentinc/ide-sidecar.git (fetch)
    upstream  git@github.com:confluentinc/ide-sidecar.git (push)

#### Get the latest upstream code

Once setup, you can periodically sync your fork with the upstream repository, using just a few Git commands. The most common way is to keep your local `main` branch always in sync with the _upstream_ repository's `main` branch:

    git checkout main
    git fetch upstream
    git pull upstream main

You can create local branches from `main` and do your development there.

> [!NOTE]  
> You don't need to keep the `main` branch on your remote https://github.com/YOUR-USERNAME/ide-sidecar repository in sync, but you can if you want:
>
>     git push origin main

For more details and other options, see "[Syncing a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork)" in GitHub's documentation.

#### Install SDKMAN! and GraalVM

Now that you have the source code, you can install the other tools.

We use the community edition of GraalVM, called GraalVM CE, and we use SDKMAN! to install the version of GraalVM CE defined in the file `.sdkmanrc`. If you do not have SDKMAN! installed yet, please do so by [following the installation instructions](https://sdkman.io/install) or [install SDKMAN! via brew](https://github.com/sdkman/homebrew-tap).

Then install GraalVM CE with SDKMAN! by running the following command in the root directory of the project:

    sdk env install


#### Security and pre-commit

Accidentally checking in secrets into any Git repository is no fun for anyone involved, so we use [`pre-commit`](https://pre-commit.com) hooks and `gitleaks` to prevent this from happening. Please install `pre-commit` hooks locally to check all commits you make locally. You can install it [with PIP](https://pre-commit.com/#install):

    pip install pre-commit

or [with brew](https://formulae.brew.sh/formula/pre-commit):

    brew install pre-commit

Confirm the installed version is at least 2.13.0:

    pre-commit --version

Then, run this command inside the root directory of this repository to install the pre-commit hooks:

    pre-commit install


### Building locally

Now that you have the source code and installed all the tools, you can build the project locally.
First check out the `main` branch:

    git checkout main

and pull the latest changes from the _project's repository_:

    git pull upstream main

To compile the Java code, check the code against style conventions and potential bugs, assemble the JAR files, build the native executable, and _skip tests_:

    make build

To compile the Java code _and_ run the Java unit and integration tests but _not_ test the native executable:

    make test

To compile the Java code, build the native executable, and run the tests _against the native executable_:

    make test-native

You can then execute your native executable with: `./target/ide-sidecar-*-runner`
If you want to learn more about building native executables, please consult https://quarkus.io/guides/maven-tooling.


### Cleaning

The build will create a lot of local files. You can clean up these generated files with:

    make clean

Cleaning is often useful to ensure that all generated files, JARs and executables are removed, before rerunning
the build and tests.

### Testing

This project uses unit tests and integration tests to verify functionality and identify regressions. 

#### Unit tests

Unit tests are located in the `src/test/java` directory in the same packages as the code they test,
defined in files ending with `Test.java`.
Unit tests use [JUnit 5](https://junit.org/).
Unit tests should test small, isolated classes and functionality, and should not be unnecessarily complex. For example, they will mock components used by the code under test.

#### Integration tests

Integration tests are located in the `src/test/java` directory in the same packages as the code they test,
defined in files ending with `IT.java` and have the annotation `@Tag("io.confluent.common.utils.IntegrationTest")`.
Integration tests also use [JUnit 5](https://junit.org/).
Integration tests run components configured as they would be in production, but run against containerized
external services, such as a Kafka broker or Schema Registry instance.

#### Running the tests

As mentioned above, to run all tests against the Java code (compiled as JARs):

    make test

To build the native executable and run the integration tests against it:

    make test-native

The native executable runs in the `PROD` profile and does not have access to, for instance,
Quarkus dev services. The integration tests can be configured in the file
`./src/test/resources/application-nativeit.yaml`.


#### Running the Application in dev mode for continuous and manual testing

You can build and run your application in "dev mode", which enables live coding with hot reloads.
This uses Quarkus' [continuous testing](https://quarkus.io/guides/continuous-testing), so you can get instant feedback on your changes.
The following command starts "dev mode" to compile the code, and immediately run the relevant tests as soon as you save changes to the code:

    ./mvnw quarkus:test

Failed tests will be highlighted, but they do not stop "dev mode".

While "dev mode" is running, you can use the Quarkus Dev UI that is available at http://localhost:26636/q/dev-ui.
You can also use a different terminal and `curl` to hit the REST and GraphQL APIs at http://localhost:26636/gateway/v1/.


##### OpenAPI specification

The OpenAPI specification for the API is generated as part of the normal build into the `src/generated/resources` directory.

The YAML OpenAPI specification is also available when running the application at http://localhost:26636/openapi:

    curl -s http://localhost:26636/openapi

Add the `format=json` query parameter to get the JSON formatted specification:

    curl -s http://localhost:26636/openapi?format=json

##### Swagger UI

The [Swagger UI](https://swagger.io/tools/swagger-ui/) helps visualize and interact with the APIs.

The Swagger UI is available in dev mode by pointing your browser at http://localhost:26636/swagger-ui. 
By default, Swagger UI is only available when Quarkus is started in dev or test mode.

##### GraphQL API

The GraphQL endpoint at http://localhost:26636/gateway/v1/graphql can be used to query the CCloud or local connections, and the clusters and other resources available through those connections.
The GraphQL Schema for this endpoint is generated as part of the normal build into the `src/generated/resources` directory.

The [Quarkus DevUI](http://localhost:26636/q/dev-ui) contains a user interface for [working with GraphQL](http://localhost:26636/q/graphql-u), and is only enabled in dev mode.


## Using Sidecar

This section shows how to use `curl` to interact with a running Sidecar.

### Handshake

Sidecar uses authentication to prevent access from unauthorized clients. The sidecar returns an
authentication token from the first invocation of the `/gateway/v1/handshake` route. Use this
authentication token to sign API requests.

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
Initially, the Sidecar does not hold any connection
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
will include the CCloud connection and its `status.authentication.status` will be `VALID_TOKEN`, signifying the session has been authenticated:

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

#### List Kafka topics using the Kafka Proxy API

Assuming the Kafka cluster ID is `lkc-abcd123`, we can list the topics:

```shell
curl -s \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  http://localhost:26636/kafka/v3/clusters/lkc-abcd123/topics | jq -r .
```

#### Create a Kafka topic using the Kafka Proxy API

Assuming the cluster ID is `lkc-abcd123`, we can create a Kafka topic with the following command:

```shell
curl -s -X POST \
  -H "Content-Type:application/json" \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  -d '{"topic_name": "foo"}' \
  http://localhost:26636/kafka/v3/clusters/lkc-abcd123/topics | jq -r .
```

#### List Schemas using the Schema Registry Proxy API

Assuming the Schema Registry cluster ID is `lsrc-defg456`, we can list the schemas with the following command:

```shell
curl -s \
  -H "Authorization: Bearer ${DTX_ACCESS_TOKEN}" \
  -H "x-connection-id: c1" \
  -H "x-cluster-id: lsrc-defg456" \
  http://localhost:26636/schemas?subjectPrefix=:*:&latestOnly=true | jq -r .
```
