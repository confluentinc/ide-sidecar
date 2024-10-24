# Python Producer

This project contains a Python 3 application that writes to a topic on Confluent Cloud.

## Getting Started

### Prerequisites

We assume that you already have Python 3 installed. The template was last tested against Python 3.12.2.

The instructions use `virtualenv` but you may use other virtual environment managers like `venv` if you prefer.

### Installation

Create and activate a Python environment, so that you have an isolated workspace:

```shell
$ virtualenv env
$ source env/bin/activate
```

Install the dependencies of this application:

```shell
$ pip install -r requirements.txt
```

Make the producer script executable:

```shell
$ chmod u+x producer.py
```

### Usage

You can execute the producer script by running:

```shell
$ ./producer.py
```

## Troubleshooting

### Running `pip install -r requirements.txt` fails

If the execution of `pip install -r requirements.txt` fails with an error message indicating that librdkafka cannot be
found, please check if you are using a Python version for which a
[built distribution](https://pypi.org/project/confluent-kafka/2.3.0/#files) of `confluent-kafka` is available.