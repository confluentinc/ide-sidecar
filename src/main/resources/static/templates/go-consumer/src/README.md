# Go Consumer

This project contains a Go application that subscribes to a topic on Confluent Cloud and prints the
consumed records to the console.

## Getting Started

### Prerequisites

We assume that you already have Go installed. The template was last tested against Go 1.22.2.

### Installation

Install the dependencies of this application, which are defined in the file `go.mod`:

```shell
$ go mod download
```

### Usage

You can compile and run the consumer application by executing:

```shell
$ go run consumer.go
```

If you want to just compile but not run the application, you can execute:

```shell
$ go build
```

This creates the file `go-consumer`, which you can execute as follows:

```shell
$ ./go-consumer
```