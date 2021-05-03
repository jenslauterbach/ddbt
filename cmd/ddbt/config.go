package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// arguments stores the command line options and arguments the user provides when invoking the program.
type arguments struct {
	// region is the AWS region the DynamoDB table is in (--region).
	region string
	// profile is the AWS profile to use when determining the users credentials (--profile).
	profile string
	// endpoint is the DynamoDB endpoint to which API calls are send to (--endpoint-url).
	endpoint string
	// table is the name of the DynamoDB table to truncate.
	table string
	// retries is the number of retries the underlying HTTP client should do, if a request fails. The default is set
	// by defaultMaxRetries and can be overwritten with --max-retries.
	retries int
	// debug indicates whether or not the program should show debugging output (--debug).
	debug bool
	// help indicates whether or not the program should show the help/usage (--help).
	help bool
	// version indicates whether or not the program should show the version number (--version).
	version bool
	// dryRun indicates whether or not the program should perform a dry run (--dry-run).
	dryRun bool
	// noInput indicates whether or not the user should be asked for any input or not (--no-input).
	noInput bool
	// quiet indicates whether or not the program should show any output (--quiet).
	quiet bool
	// disableColor indicates whether or not the programs output should be colored or not. If set to 'true', the output
	// will not be colored. If set to 'false' (the default), output is colored. Set by the --no-color flag.
	disableColor bool
}

// parseArguments does parse the given flag set and command line args.
//
// If the parsing is successful, the function returns the arguments and a nil error. Otherwise, the function returns a
// non-nil error and "empty" arguments.
func parseArguments(flags *flag.FlagSet, args []string) (arguments, error) {
	region := flags.String("region", "", "AWS region to use")
	profile := flags.String("profile", "", "AWS profile to use")
	endpoint := flags.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	retries := flags.Int("max-retries", defaultMaxRetries, fmt.Sprintf("maximum number of retries (default: %d)", defaultMaxRetries))
	debug := flags.Bool("debug", false, "show debug information")
	help := flags.Bool("help", false, "show help text")
	showVersion := flags.Bool("version", false, "show version")
	dry := flags.Bool("dry-run", false, "run command without actually deleting items")
	noInput := flags.Bool("no-input", false, "Do not require any input")
	quiet := flags.Bool("quiet", false, "Disable all output (except for required input)")
	noColor := flags.Bool("no-color", false, "Disable colored output")

	err := flags.Parse(args)
	if err != nil {
		return arguments{}, err
	}

	table := flags.Arg(0)
	if isInputFromPipe() {
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return arguments{}, errReadPipe
		}
		table = string(b)
	}

	return arguments{
		region:       *region,
		profile:      *profile,
		endpoint:     *endpoint,
		table:        table,
		retries:      *retries,
		debug:        *debug,
		help:         *help,
		version:      *showVersion,
		dryRun:       *dry,
		noInput:      *noInput,
		quiet:        *quiet,
		disableColor: *noColor,
	}, nil
}

// configuration contains the most important settings relevant to the programs function.
type configuration struct {
	// table is the name of the DynamoDB table to be truncated.
	table string
	// db is the client used to interact with DynamoDB.
	db DynamoDBAPI
	// logger is used to output debug information.
	logger *log.Logger
	// dryRun allows running the program without actually deleting items from DynamoDB.
	dryRun bool
	// stats keeps track of deleted of important statistics related to the process of truncating the table, like number
	// of deleted or failed items or how much capacity was consumed.
	stats *statistics
}

// newAwsConfig returns a AWS configuration based on the options the user selected on the command line.
func newAwsConfig(args arguments) (aws.Config, error) {
	// The options array will be updated within this function based on the provided command line arguments. At the end
	// the array will be passed to 'LoadDefaultConfig()' to create a AWS configuration for the DynamoDB client that
	// reflects the options the user has selected on the command line.
	var options []func(*config.LoadOptions) error

	if args.region != "" {
		options = append(options, config.WithRegion(args.region))
	}

	if args.profile != "" {
		options = append(options, config.WithSharedConfigProfile(args.profile))
	}

	options = append(options, config.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), 5)
	}))

	if args.endpoint != "" {
		resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == dynamodb.ServiceID {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           args.endpoint,
					SigningRegion: region,
				}, nil
			}

			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

		options = append(options, config.WithEndpointResolver(resolver))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), options...)
	if err != nil {
		return aws.Config{}, err
	}

	return cfg, nil
}

// newConfig creates a new configuration from the given arguments.
//
// If the configuration is created successfully the function will return the configuration and a nil error. Otherwise,
// the function returns a non-nil error and an empty configuration.
func newConfig(args arguments) (configuration, error) {
	awsConfig, err := newAwsConfig(args)
	if err != nil {
		return configuration{}, err
	}

	return configuration{
		table:  args.table,
		db:     dynamodb.NewFromConfig(awsConfig),
		dryRun: args.dryRun,
		stats:  &statistics{},
	}, nil
}

func isInputFromPipe() bool {
	fileInfo, _ := os.Stdin.Stat()
	return fileInfo.Mode()&os.ModeCharDevice == 0
}

// The following constants are used for reading environment variables to find out if color should be used for output.
const (
	// envVariableSeparator is the separator used to split environment variables strings returned by os.Environ()
	envVariableSeparator = "="

	// envVariableIndexName is the index for the environment variable name in the slice resulting from splitting a
	// environment variable string from os.Environ() by the separator defined by envVariableSeparator.
	envVariableIndexName = 0

	// envVariableIndexName is the index for the environment variable value in the slice resulting from splitting a
	// environment variable string from os.Environ() by the separator defined by envVariableSeparator.
	envVariableIndexValue = 1

	// envVariableNoColor is the name of the NO_COLOR environment variable.
	envVariableNoColor = "NO_COLOR"

	// envVariableDDBTNoColor is the name of the DDBT_NO_COLOR environment variable.
	envVariableDDBTNoColor = "DDBT_NO_COLOR"

	// envVariableTerm is the name of the TERM environment variable.
	envVariableTerm = "TERM"

	// termModeDumb is the name of the "dumb" mode which is a possible value of the TERM environment
	// variable (see envVariableTerm). This value will disable colored output.
	termModeDumb = "dump"
)

// disableColor determines whether or not the output of the program should be colored or not. The function considers the
// given value of the associated command line option '--no-color' and the programs environment.
//
// Besides the command line option '--no-color' the following environment variables are considered:
//
// 	1. NO_COLOR (any value)
//  2. DDBT_NO_COLOR (any value)
//	3. TERM (if set to 'dumb')
//
// If any of those environment has the expected value, ths function will return 'true'. Only if none of those
// environment variables exist or are set to the appropriate value and the command line option '--no-color' is set to
// 'false', then the function will return 'false'.
func disableColor(disableColor bool, environment []string) bool {
	if disableColor {
		return true
	}

	for _, variable := range environment {
		s := strings.Split(variable, envVariableSeparator)
		name := s[envVariableIndexName]

		switch name {
		case envVariableNoColor:
			return true
		case envVariableDDBTNoColor:
			return true
		case envVariableTerm:
			if len(s) > 1 && s[envVariableIndexValue] == termModeDumb {
				return true
			}
		}
	}

	return false
}
