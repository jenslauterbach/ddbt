package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pterm/pterm"
	"strconv"

	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
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
	defaultMaxRetries = 3

	// usage is the message that is displayed when the user explicitly uses the --help flag or when there is an error
	// and the user is to be informed about proper usage of ddbt.
	usage = `Usage: ddbt [options...] <table-name>

Options:
  --debug		Show debug information
  --dry-run		Simulate truncating table
  --endpoint-url	Custom endpoint url (overwrite default endpoint)
  --help		This help text
  --max-retries		Maximum number of retries (default: 3)
  --no-input		Do not require any input
  --profile		AWS profile to use
  --region		AWS region of DynamoDB table (overwrite default region)
  --version		Show version number and quit
`
)

var (
	// The following variables are set by goreleaser:
	version = "dev"
	date    = "unknown"

	errTableMissing = errors.New("no table name provided")
	errReadPipe     = errors.New("unable to read table name from pipe (stdin)")
)

func main() {
	err := run(os.Args[1:])
	if err != nil {
		pterm.Error.Printf("%s\n\n", err.Error())
		pterm.Printf("%s", usage)
		os.Exit(1)
	}
}

func run(args []string) error {
	start := time.Now()

	parsedArguments, err := parseArguments(flag.CommandLine, args)
	if err != nil {
		return err
	}

	if parsedArguments.help {
		pterm.Printf("%s", usage)
		return nil
	}

	if parsedArguments.version {
		// Version information should be printed as plain text to make scripting easier.
		pterm.Printf("ddbt %s (built at %s)\n", version, date)
		return nil
	}

	if parsedArguments.table == "" {
		return errTableMissing
	}

	if parsedArguments.dryRun {
		pterm.Info.Println("Performing dry run")
	}

	if parsedArguments.debug {
		pterm.EnableDebugMessages()
	}

	conf, err := newConfig(parsedArguments)
	if err != nil {
		return err
	}

	tableInfo, err := retrieveTableInformation(conf)
	if err != nil {
		return err
	}

	if !parsedArguments.noInput {
		reader := bufio.NewReader(os.Stdin)
		ok := askForConfirmation(reader, tableInfo)
		if !ok {
			return nil
		}
	}

	defer printStatistics(conf.stats, start)

	err = truncateTable(context.Background(), conf, tableInfo)
	if err != nil {
		return err
	}

	return nil
}

func askForConfirmation(reader io.RuneReader, tableInfo *dynamodb.DescribeTableOutput) bool {
	pterm.Warning.Printf("Do you really want to delete approximately %d items from table %s? [Y/n] ", tableInfo.Table.ItemCount, *tableInfo.Table.TableArn)

	input, _, err := reader.ReadRune()
	if err != nil {
		pterm.Println("Unable to read your input. Aborting truncate operation. For more detail run with --debug.")
		pterm.Debug.Printf("error: %v\n", err.Error())
		return false
	}

	switch input {
	case 'Y':
		return true
	case 'n':
		pterm.Println("You selected 'n'. Aborting truncate operation.")
		return false
	default:
		pterm.Println("Neither 'Y' nor 'n' selected. Aborting truncate operation.")
		return false
	}
}

func printStatistics(stats *statistics, start time.Time) {
	pterm.DefaultSection.Println("Statistics")

	err := pterm.DefaultTable.WithData(pterm.TableData{
		{"Deleted items:", strconv.FormatUint(stats.deleted, 10)},
		{"Duration:", time.Since(start).String()},
		{"Consumed Read Capacity Units:", strconv.FormatFloat(stats.rcu, 'f', 6, 64)},
		{"Consumed Write Capacity Units:", strconv.FormatFloat(stats.wcu, 'f', 6, 64)},
	}).Render()
	if err != nil {
		pterm.Error.Printf("Unable to print statistics table: %v\n", err)
	}
}

type arguments struct {
	region   string
	profile  string
	endpoint string
	table    string
	retries  int
	debug    bool
	help     bool
	version  bool
	dryRun   bool
	noInput  bool
}

func parseArguments(flags *flag.FlagSet, args []string) (arguments, error) {
	region := flags.String("region", "", "AWS region to use")
	profile := flags.String("profile", "", "AWS profile to use")
	endpoint := flags.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	retries := flags.Int("max-retries", defaultMaxRetries, fmt.Sprintf("maximum number of retries (default: %d)", defaultMaxRetries))
	debug := flags.Bool("debug", false, "show debug information")
	help := flags.Bool("help", false, "show help text")
	version := flags.Bool("version", false, "show version")
	dry := flags.Bool("dry-run", false, "run command without actually deleting items")
	noInput := flags.Bool("no-input", false, "Do not require any input")

	err := flags.Parse(args)
	if err != nil {
		return arguments{}, err
	}

	var table string
	if isInputFromPipe() {
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return arguments{}, errReadPipe
		}
		table = string(b)
	} else {
		table = flags.Arg(0)
	}

	return arguments{
		region:   *region,
		profile:  *profile,
		endpoint: *endpoint,
		table:    table,
		retries:  *retries,
		debug:    *debug,
		help:     *help,
		version:  *version,
		dryRun:   *dry,
		noInput:  *noInput,
	}, nil
}

func isInputFromPipe() bool {
	fileInfo, _ := os.Stdin.Stat()
	return fileInfo.Mode()&os.ModeCharDevice == 0
}

type configuration struct {
	// table is the name of the DynamoDB table to be truncated
	table string
	// db is the client used to interact with DynamoDB
	db DynamoDBAPI
	// log is used to output debug information
	logger *log.Logger
	// dryRun allows running the program without actually deleting items from DynamoDB
	dryRun bool
	// stats keeps track of deleted of important statistics related to the process of truncating the table, like number
	// of deleted or failed items or how much capacity was consumed.
	stats *statistics
}

type statistics struct {
	mu sync.Mutex
	// deleted number of items
	deleted uint64
	// rcu is the number of consumed read capacity units
	rcu float64
	// wcu is the number of consumed write capacity units
	wcu float64
}

func (s *statistics) increaseDeleted(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted += n
}

func (s *statistics) addRCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rcu += n
}

func (s *statistics) addWCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wcu += n
}

func newConfig(args arguments) (configuration, error) {
	awsConfig := newAwsConfig(args)

	return configuration{
		table:  args.table,
		db:     dynamodb.NewFromConfig(awsConfig),
		dryRun: args.dryRun,
		stats:  &statistics{},
	}, nil
}

// newAwsConfig returns a AWS configuration based on the options the user selected on the command line.
func newAwsConfig(args arguments) aws.Config {
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

	// TODO: handle error.
	cfg, _ := config.LoadDefaultConfig(context.TODO(), options...)

	return cfg
}

func retrieveTableInformation(config configuration) (*dynamodb.DescribeTableOutput, error) {
	pterm.Debug.Printf("retrieving table information for table %s\n", config.table)

	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(config.table),
	}
	pterm.Debug.Printf("DescribeTableInput: %v\n", params)

	output, err := config.db.DescribeTable(context.TODO(), params)
	if err != nil {
		return nil, fmt.Errorf("unable to get table information for table %s: %w", config.table, err)
	}
	pterm.Debug.Printf("DescribeTableOutput: %v\n", *output)

	return output, nil
}

func truncateTable(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput) error {
	pterm.Info.Printf("Truncating table %s\n", *tableInfo.Table.TableArn)

	g, ctx := errgroup.WithContext(ctx)

	segments := 4
	for segment := 0; segment < segments; segment++ {
		pterm.Debug.Printf("start segment %d of %d\n", segment, segments)

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

	pterm.Success.Printf("Truncated table %s\n", *tableInfo.Table.TableArn)

	return nil
}

func processSegment(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput, totalSegments int, segment int, g *errgroup.Group) error {
	pterm.Debug.Printf("start processing segment %d\n", segment)

	expr, err := newProjection(tableInfo.Table.KeySchema)
	if err != nil {
		return err
	}

	params := &dynamodb.ScanInput{
		TableName:                aws.String(config.table),
		ProjectionExpression:     expr.Projection(),
		ExpressionAttributeNames: expr.Names(),
		TotalSegments:            aws.Int32(int32(totalSegments)),
		Segment:                  aws.Int32(int32(segment)),
		ReturnConsumedCapacity:   types.ReturnConsumedCapacityTotal,
	}
	pterm.Debug.Printf("ScanInput: %v\n", params)

	paginator := dynamodb.NewScanPaginator(config.db, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			// TODO: Report error and keep going? Add fail fast flag?
			continue
		}

		config.stats.addRCU(*page.ConsumedCapacity.CapacityUnits)

		g.Go(func() error {
			return processPage(ctx, config, page)
		})
	}

	pterm.Debug.Printf("finish processing segment %d\n", segment)
	return nil
}

func newProjection(keys []types.KeySchemaElement) (*expression.Expression, error) {
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
	var total = page.Count
	var from int32
	var to int32
	for from = 0; from < total; from += batchSize {
		to += batchSize
		if to > total {
			to = total
		}

		err := deleteBatch(ctx, config, page.Items[from:to])
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteBatch(ctx context.Context, config configuration, items []map[string]types.AttributeValue) error {
	bSize := uint64(len(items))
	var processed uint64

	requests := make([]types.WriteRequest, bSize)

	for index, key := range items {
		requests[index] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: key,
			},
		}
	}

	unprocessed := map[string][]types.WriteRequest{config.table: requests}
	for ok := true; ok; ok = len(unprocessed) > 0 {
		params := &dynamodb.BatchWriteItemInput{
			RequestItems:           unprocessed,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}

		if !config.dryRun {
			output, err := config.db.BatchWriteItem(ctx, params)
			if err != nil {
				return fmt.Errorf("unable to send delete requests: %w", err)
			}

			for _, u := range output.ConsumedCapacity {
				config.stats.addWCU(*u.CapacityUnits)
			}

			unprocessed = output.UnprocessedItems
		} else {
			unprocessed = map[string][]types.WriteRequest{}
		}

		processed = bSize - processed - uint64(len(unprocessed))
		config.stats.increaseDeleted(processed)
	}

	return nil
}

type DynamoDBAPI interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}
