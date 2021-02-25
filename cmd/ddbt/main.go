package main

import (
	"bufio"
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
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"text/tabwriter"
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
		fmt.Fprintf(os.Stderr, "error: %s\n\n%s", err.Error(), usage)
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
		fmt.Fprintf(os.Stdout, "%s", usage)
		return nil
	}

	if parsedArguments.version {
		fmt.Fprintf(os.Stdout, "ddbt %s (built at %s)\n", version, date)
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

	if !parsedArguments.noninteractive {
		reader := bufio.NewReader(os.Stdin)
		ok := askForConfirmation(config, reader, tableInfo)
		if !ok {
			return nil
		}
	}

	defer printStatistics(config.stats, start)

	err = truncateTable(context.Background(), config, tableInfo)
	if err != nil {
		return err
	}

	return nil
}

func askForConfirmation(config configuration, reader io.RuneReader, tableInfo *dynamodb.DescribeTableOutput) bool {
	fmt.Printf("Do you really want to delete approximatelly %d items from table %s? [Y/n] ", *tableInfo.Table.ItemCount, *tableInfo.Table.TableArn)

	input, _, err := reader.ReadRune()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to read your input. Aborting truncate operation. For more detail run with --debug.")
		config.logger.Printf("error: %v\n", err.Error())
		return false
	}

	switch input {
	case 'Y':
		return true
	case 'n':
		fmt.Fprintln(os.Stderr, "You selected 'n'. Aborting truncate operation.")
		return false
	default:
		fmt.Fprintln(os.Stderr, "Neither 'Y' nor 'n' selected. Aborting truncate operation.")
		return false
	}
}

func printStatistics(stats *statistics, start time.Time) {
	fmt.Printf("\nStatistics:\n\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.DiscardEmptyColumns)
	fmt.Fprintf(w, "Deleted items:\t%d\n", stats.deleted)
	fmt.Fprintf(w, "Duration:\t%v\n", time.Since(start))
	fmt.Fprintf(w, "Consumed Read Capacity Units:\t%v\n", stats.rcu)
	fmt.Fprintf(w, "Consumed Write Capacity Units:\t%v\n", stats.wcu)
	w.Flush()
}

type arguments struct {
	region         string
	endpoint       string
	table          string
	retries        int
	debug          bool
	help           bool
	version        bool
	dryRun         bool
	noninteractive bool
}

func parseArguments(flags *flag.FlagSet, args []string) (arguments, error) {
	region := flags.String("region", "", "AWS region to use")
	endpoint := flags.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	retries := flags.Int("max-retries", defaultMaxRetries, fmt.Sprintf("maximum number of retries (default: %d)", defaultMaxRetries))
	debug := flags.Bool("debug", false, "show debug information")
	help := flags.Bool("help", false, "show help text")
	version := flags.Bool("version", false, "show version")
	dry := flags.Bool("dry-run", false, "run command without actually deleting items")
	noninteractive := flags.Bool("non-interactive", false, "run command without actually deleting items")

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
		region:         *region,
		endpoint:       *endpoint,
		table:          table,
		retries:        *retries,
		debug:          *debug,
		help:           *help,
		version:        *version,
		dryRun:         *dry,
		noninteractive: *noninteractive,
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
	db dynamodbiface.DynamoDBAPI
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
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return configuration{}, fmt.Errorf("unable to create new session: %w", err)
	}

	var logOutput = ioutil.Discard
	if args.debug {
		logOutput = os.Stdout
	}

	return configuration{
		table:  args.table,
		db:     dynamodb.New(sess),
		logger: log.New(logOutput, "debug: ", log.Ldate|log.Ltime|log.Lmicroseconds),
		dryRun: args.dryRun,
		stats:  &statistics{},
	}, nil
}

func newAwsConfig(args arguments) *aws.Config {
	config := &aws.Config{}

	if args.region != "" {
		config.Region = &args.region
	}

	config.MaxRetries = aws.Int(args.retries)

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
	fmt.Fprintf(os.Stdout, "Truncating table %s\n", *tableInfo.Table.TableArn)

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
		ReturnConsumedCapacity:   aws.String("TOTAL"),
	}
	config.logger.Printf("ScanInput: %s\n", input)

	err = config.db.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		config.stats.addRCU(*page.ConsumedCapacity.CapacityUnits)

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

		err := deleteBatch(ctx, config, page.Items[from:to])
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteBatch(ctx context.Context, config configuration, items []map[string]*dynamodb.AttributeValue) error {
	bSize := uint64(len(items))
	var processed uint64

	requests := make([]*dynamodb.WriteRequest, bSize)

	for index, key := range items {
		requests[index] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: key,
			},
		}
	}

	unprocessed := map[string][]*dynamodb.WriteRequest{config.table: requests}
	for ok := true; ok; ok = len(unprocessed) > 0 {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems:           unprocessed,
			ReturnConsumedCapacity: aws.String("TOTAL"),
		}

		if !config.dryRun {
			output, err := config.db.BatchWriteItemWithContext(ctx, input)
			if err != nil {
				return fmt.Errorf("unable to send delete requests: %w", err)
			}

			for _, u := range output.ConsumedCapacity {
				config.stats.addWCU(*u.CapacityUnits)
			}

			unprocessed = output.UnprocessedItems
		} else {
			unprocessed = map[string][]*dynamodb.WriteRequest{}
		}

		processed = bSize - processed - uint64(len(unprocessed))
		config.stats.increaseDeleted(processed)
	}

	return nil
}
