# DynamoDB Truncate (ddbt)

> Disclaimer: This tool is in early development. It is *not* recommended to use this tool in production. There is the risk of data loss.

> :warning: :bangbang: 
> Important:
>
>   1. This tool has the potential to create a lot of AWS cost.
>   2. Deleting and re-creating the table is cheaper and faster.

**ddbt** is a simple command line tool that does one job and one job only: delete all items in a [AWS DynamoDB](https://aws.amazon.com/dynamodb/) table. This tool is made for situations where you want to delete all items in a table, but do not want to delete the table. If you can delete and re-create the table, it is recommend to do this. It will be faster and cheaper.

## Table of Contents

- [Installation](#installation)
  - [Download from Github](#download-from-github)
  - [Using Homebrew](#using-homebrew)
  - [Building From Source](#building-from-source)
- [Configuration](#configuration)
- [Usage](#usage)
- [Quick Start](#quick-start)
- [Permissions](#permissions)
- [AWS Cost](#aws-cost)
- [Design Goals](#design-goals)
- [Versioning](#versioning)
- [Authors](#authors)
- [License](#license)

## Installation
[(Back to top)](#table-of-contents)

### Download from Github

You can go to the release page and download a pre-compiled binary for your operating system: [Release page](https://github.com/jenslauterbach/ddbt/releases/latest)

### Using Homebrew

To install **ddbt** using [brew](https://brew.sh) on macOS or supported Linux distributions, run the following commands:

```shell script
brew tap jenslauterbach/ddbt
brew install ddbt
```

### Building From Source

To build from source you need to install [Go](https://golang.org) first.

```shell
git clone https://github.com/jenslauterbach/ddbt.git
cd ddbt
go build ./cmd/ddbt
```

The build will create a new binary called `ddbt`.

## Configuration
[(Back to top)](#table-of-contents)

**ddbt** behaves the same way as the `aws` cli. Credentials (access key and secret access key) can be set in different ways, like `~/.aws/credentials`, environment variables or IAM roles (ECS/EC2). Only the region and endpoint URL can be changed through command line flags.

## Usage
[(Back to top)](#table-of-contents)

To truncate a DynamoDB table the only required _argument_ is the name of the table. Everything else can be controlled through options (also called flags):

````shell script
Usage: ddbt [options...] <table-name>

Options:
    -d, --debug                 Show debug information
        --dry-run               Simulate truncating table
        --endpoint-url <url>    Custom endpoint url (overwrite default endpoint)
    -h, --help                  This help text
        --max-retries <retries> Maximum number of retries (default: 3)
        --no-input              Do not require any input
        --no-color              Disable colored output
    -p, --profile <profile>     AWS profile to use
    -q, --quiet                 Disable all output (except for required input)
    -r, --region <region>       AWS region of DynamoDB table (overwrite default region)
        --version               Show version number and quit

Examples:

    $ ddbt TestTable
    $ ddbt --region eu-central-1 TestTable
    $ ddbt --region localhost --endpoint-url http://localhost:8000 TestTable
````

## Quick Start
[(Back to top)](#table-of-contents)

The following examples assume that the table that should be truncated is called `SomeTable`.

**Truncate Table:**
```shell script
ddbt SomeTable
```

**Dry Run**:
```shell script
ddbt --dry-run SomeTable
```

**Change Region:**
```shell script
ddbt --region eu-central-1 SomeTable
```

## Permissions
[(Back to top)](#table-of-contents)

The following table lists the IAM permissions a user needs to use `ddbt`.

| Permission | Description |
|:---|:---|
|`dynamodb:DescribeTable`|Required to find out how many items (approximately) are in the table. This number is displayed, when the user is asked for confirmation. Furthermore, this operation is used to find out what the keys of the table are.|
|`dynamodb:Scan`|Required to read all items in the table, to then delete them (using `BatchWriteItem`).|
|`dynamodb:BatchWriteItem`|Required to delete items from the table.|


## AWS Cost
[(Back to top)](#table-of-contents)

Running **ddbt** will cause costs, because of the way **ddbt** works. First a `Scan` is performed to get the keys for _every_ item in the table. Then a `BatchWrite` is performed, to delete those items. This means that read and write capacity units (RCU, WCU) will be used.

The actual cost depends on your capacity mode (on-demand or provisioned).

## Design Goals
[(Back to top)](#table-of-contents)

The primary design goals of **ddbt** are:

1. Be as compatible to the aws cli as possible in terms of UX (flags)
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
