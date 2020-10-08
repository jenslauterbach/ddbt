# ddbt (DynamoDB Truncate)

> Disclaimer: This tool is in early development. It is *not* recommend to use this tool in production. There is the risk of data loss.

**ddbt** is a simple command line tool that does one job and one job only: delete all items from a [AWS DynamoDB](https://aws.amazon.com/dynamodb/) table.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Flags and Arguments](#flags-and-arguments)
- [Usage](#usage)
- [Design Goals](#design-goals)
- [Versioning](#versioning)
- [Authors](#authors)
- [License](#license)

## Installation
[(Back to top)](#table-of-contents)

## Configuration
[(Back to top)](#table-of-contents)

**ddbt** behaves the same way as the `aws` cli. Credentials (access key and secret access key) can be set in different ways, like `~/.aws/credentials`, environment variables or IAM roles (ECS/EC2). Only the region and endpoint URL can be changed through command line flags.

## Flags and Arguments
[(Back to top)](#table-of-contents)

## Usage
[(Back to top)](#table-of-contents)

## Design Goals
[(Back to top)](#table-of-contents)

The primary design goals of **ddbt** are:

1. Be as compatible to the aws cli as possible in terms of UX (flags, outputs).
2. Cross platform
3. Minimal AWS cost

## Versioning

[(Back to top)](#table-of-contents)

This project uses SemVer for versioning. For the versions available, see the [releases](https://github.com/jenslauterbach/ddbt/releases) page.

## Authors

[(Back to top)](#table-of-contents)

- Jens Lauterbach - Main author - (@jenslauterbach)

## License

[(Back to top)](#table-of-contents)

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/jenslauterbach/ddbt/blob/master/LICENSE) file for details.
