package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"log"
	"math"
	"os"
	"time"
)

const (
	// batchSize is the maximum number of items in a batch request send to DynamoDB. The maximum batch size is 25 at
	// the moment.
	//
	// AWS documentation:
	//
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-api
	batchSize = 25

	// defaultMaxRetries is the default number of times the program will retry failed AWS requests. The value can be
	// overwritten using the --max-retries command line argument.
	defaultMaxRetries = 10

	// backoffBase is the "base" used by the exponential backoff algorithm to determine when the next request should be
	// send.
	backoffBase = 1.5

	// usage is the message that is displayed when the user explicitly uses the --help flag or when there is an error
	// and the user is to be informed about proper usage of ddbt.
	usage = `Usage: ddbt [options...] <table-name>

Options:
  --region		AWS region of DynamoDB table (overwrite default region)
  --endpoint-url	Custom endpoint url (overwrite default endpoint)
  --max-retries		Maximum number of retries (default: 10)
  --help		This help text
  --version		Show version number and quit
`
)

var (
	// commandVersion is the version of the ddbt tool, which is set at build time using ldflags
	commandVersion  = "development"
	errTableMissing = errors.New("no table name provided")
)

func main() {
	err := run(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n\n%s", err.Error(), usage)
		os.Exit(1)
	}
}

func run(args []string) error {
	parsedArguments, err := parseArguments(flag.CommandLine, args)
	if err != nil {
		return err
	}

	if parsedArguments.help {
		fmt.Fprintf(os.Stdout, "%s", usage)
		return nil
	}

	if parsedArguments.version {
		fmt.Fprintf(os.Stdout, "ddbt %s\n", commandVersion)
		return nil
	}

	if parsedArguments.table == "" {
		return errTableMissing
	}

	if parsedArguments.dryRun {
		fmt.Fprintf(os.Stdout, "Performing dry run\n")
	}

	config, err := newConfig(parsedArguments)
	if err != nil {
		return err
	}

	tableInfo, err := retrieveTableInformation(config)
	if err != nil {
		return err
	}

	err = truncateTable(context.Background(), config, tableInfo)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "Table %s successfully truncated", config.table)
	return nil
}

type configuration struct {
	// table is the name of the DynamoDB table to be truncated
	table string

	// db is the client used to interact with DynamoDB
	db dynamodbiface.DynamoDBAPI

	// maxRetries is the number of times a failed network request is retried
	maxRetries uint

	// log is used to output debug information
	logger *log.Logger

	dryRun bool
}

func newConfig(args arguments) (configuration, error) {
	awsConfig := newAwsConfig(args)
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return configuration{}, fmt.Errorf("unable to create new session: %w", err)
	}

	var logOutput = ioutil.Discard
	if args.debug {
		logOutput = os.Stdout
	}

	return configuration{
		table:      args.table,
		db:         dynamodb.New(sess),
		maxRetries: args.retries,
		logger:     log.New(logOutput, "debug: ", log.Ldate|log.Ltime|log.Lmicroseconds),
		dryRun:     args.dryRun,
	}, nil
}

type arguments struct {
	region   string
	endpoint string
	table    string
	retries  uint
	debug    bool
	help     bool
	version  bool
	dryRun   bool
}

func parseArguments(flags *flag.FlagSet, args []string) (arguments, error) {
	region := flags.String("region", "", "AWS region to use")
	endpoint := flags.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	retries := flags.Uint("max-retries", defaultMaxRetries, "maximum number of retries (default: 10)")
	debug := flags.Bool("debug", false, "show debug information")
	help := flags.Bool("help", false, "show help text")
	version := flags.Bool("version", false, "show version")
	dry := flags.Bool("dry-run", false, "run command without actually deleting items")

	err := flags.Parse(args)
	if err != nil {
		return arguments{}, err
	}

	table := flags.Arg(0)

	return arguments{
		region:   *region,
		endpoint: *endpoint,
		table:    table,
		retries:  *retries,
		debug:    *debug,
		help:     *help,
		version:  *version,
		dryRun:   *dry,
	}, nil
}

func newAwsConfig(args arguments) *aws.Config {
	config := &aws.Config{}

	if args.region != "" {
		config.Region = &args.region
	}

	if args.endpoint != "" {
		resolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.DynamodbServiceID {
				return endpoints.ResolvedEndpoint{
					URL:           args.endpoint,
					SigningRegion: region,
				}, nil
			}

			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}

		config.EndpointResolver = endpoints.ResolverFunc(resolver)
	}

	return config
}

func retrieveTableInformation(config configuration) (*dynamodb.DescribeTableOutput, error) {
	config.logger.Printf("retrieving table information for table %s\n", config.table)

	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(config.table),
	}
	config.logger.Printf("DescribeTableInput: %s\n", input)

	output, err := config.db.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("unable to get table information for table %s: %w", config.table, err)
	}
	config.logger.Printf("DescribeTableOutput: %s\n", output)

	return output, nil
}

func truncateTable(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput) error {
	fmt.Fprintf(os.Stdout, "Truncating table %s\n", * tableInfo.Table.TableArn)

	g, ctx := errgroup.WithContext(ctx)

	segments := 4
	for segment := 0; segment < segments; segment++ {
		config.logger.Printf("start segment %d of %d\n", segment, segments)

		total := segments
		current := segment
		g.Go(func() error {
			return processSegment(ctx, config, tableInfo, total, current, g)
		})
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func processSegment(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput, totalSegments int, segment int, g *errgroup.Group) error {
	config.logger.Printf("start processing segment %d\n", segment)

	expr, err := newProjection(tableInfo.Table.KeySchema)
	if err != nil {
		return err
	}

	input := &dynamodb.ScanInput{
		TableName:                aws.String(config.table),
		ProjectionExpression:     expr.Projection(),
		ExpressionAttributeNames: expr.Names(),
		TotalSegments:            aws.Int64(int64(totalSegments)),
		Segment:                  aws.Int64(int64(segment)),
	}
	config.logger.Printf("ScanInput: %s\n", input)

	err = config.db.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		g.Go(func() error {
			return processPage(ctx, config, page)
		})
		return true
	})
	if err != nil {
		return err
	}

	config.logger.Printf("finish processing segment %d\n", segment)
	return nil
}

func newProjection(keys []*dynamodb.KeySchemaElement) (*expression.Expression, error) {
	// There is at least one key in the table. This one will be added by default. If there is a second key, it is added
	// to the projection as well.
	projection := expression.NamesList(expression.Name(*keys[0].AttributeName))

	if len(keys) == 2 {
		projection = projection.AddNames(expression.Name(*keys[1].AttributeName))
	}

	expr, err := expression.NewBuilder().WithProjection(projection).Build()
	if err != nil {
		return nil, err
	}

	return &expr, nil
}

func processPage(ctx context.Context, config configuration, page *dynamodb.ScanOutput) error {
	var total = *page.Count
	var from int64
	var to int64
	for from = 0; from < total; from += batchSize {
		to += batchSize
		if to > total {
			to = total
		}

		if config.dryRun {
			continue
		}

		err := deleteBatch(ctx, config, page.Items[from:to])
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteBatch(ctx context.Context, config configuration, items []map[string]*dynamodb.AttributeValue) error {
	requests := make([]*dynamodb.WriteRequest, len(items))

	for index, key := range items {
		requests[index] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: key,
			},
		}
	}

	retry := uint(0)
	unprocessed := map[string][]*dynamodb.WriteRequest{config.table: requests}
	for ok := true; ok; ok = len(unprocessed) > 0 {
		if retry > config.maxRetries {
			break
		}

		if retry > 0 {
			sleepDuration := time.Duration(math.Pow(backoffBase, float64(retry))) * time.Second
			config.logger.Printf("sleeping for %v seconds", sleepDuration)
			time.Sleep(sleepDuration)
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: unprocessed,
		}

		output, err := config.db.BatchWriteItemWithContext(ctx, input)
		if err != nil {
			return fmt.Errorf("unable to send delete requests: %w", err)
		}

		unprocessed = output.UnprocessedItems
		retry++
	}

	return nil
}
