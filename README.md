# ddbt (DynamoDB Truncate)

> Disclaimer: This tool is in early development. It is *not* recommended to use this tool in production. There is the risk of data loss.

> :warning: :bangbang: 
> Important:
>
>   1. This tool has the potential to create a lot of AWS cost.
>   2. Deleting and re-creating the table is cheaper and faster.

**ddbt** is a simple command line tool that does one job and one job only: delete all items in a [AWS DynamoDB](https://aws.amazon.com/dynamodb/) table. This tool is made for situations where you want to delete all items in a table, but do not want to delete the table. If you can delete and re-create the table, it is recommend to do this. It will be faster and cheaper.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Quick Start](#quick-start)
- [Flags and Arguments](#flags-and-arguments)
- [AWS Cost](#aws-cost)
- [Design Goals](#design-goals)
- [Versioning](#versioning)
- [Authors](#authors)
- [License](#license)

## Installation
[(Back to top)](#table-of-contents)

## Configuration
[(Back to top)](#table-of-contents)

**ddbt** behaves the same way as the `aws` cli. Credentials (access key and secret access key) can be set in different ways, like `~/.aws/credentials`, environment variables or IAM roles (ECS/EC2). Only the region and endpoint URL can be changed through command line flags.

## Usage
[(Back to top)](#table-of-contents)

To truncate a DynamoDB table the only required _argument_ is the name of the table. Everything else can be controlled through options (also called flags):

````shell script
ddbt [options...] <table-name>
````

See chapter [Flags and Arguments](#flags-and-arguments) for an overview of all available flags.

## Flags and Arguments
[(Back to top)](#table-of-contents)

| Option | Description |
|:---|:---|
|--debug|Show debug information|
|--dry-run|Simulate truncating table|
|--endpoint-url|Custom endpoint url (overwrite default endpoint)|
|--help|Show help text|
|--max-retries|Maximum number of retries (default: 3)|
|--region|AWS region of DynamoDB table (overwrite default region)|
|--version|Show version number and quit|

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

## AWS Cost
[(Back to top)](#table-of-contents)

Running **ddbt** will cause costs, because of the way **ddbt** works. First a `Scan` is performed to get the keys for _every_ item in the table. Then a `BatchWrite` is performed, to delete those items. This means that read and write capacity units (RCU, WCU) will be used.

The actual cost depends on your capacity mode (on-demand or provisioned).

Example:

* Number of items: 1,000,000 (1 million)
* Partition key: [UUIDv4](https://tools.ietf.org/html/rfc4122) (`16fd2706-8baf-433b-82eb-8c7fada847da`)
* Sort key: [RFC3339 date](https://tools.ietf.org/html/rfc3339) (`1985-04-12T23:20:50.52Z`)
* Capacity mode: provisioned
* Read mode: eventually consistent
* Item size: max. 4kb

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
