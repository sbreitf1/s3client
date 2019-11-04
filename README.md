# S3 Client

The S3 Client can be used to connect to a preconfigured S3 Endpoint and browse it in a bash-like look and feel.

## Installation

TODO

## Getting started

TODO

## Usage

Most commands in the S3 Client console behave like in bash. Your working directory is expanded by the selected `bucket` and server including the corresponding credentials. The client is typically started using an environment, that defines which connection and default bucket to use:
```
s3client -e local
```

This command lets you enter url and credentials of a new endpoint or starts a session. You can also just call `s3client` to select the environment from a list of already configures ones.