package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pterm/pterm"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"time"
)

const (
	// maxBatchSize is the maximum number of items in a batch request send to DynamoDB. The maximum batch size is 25 at
	// the moment.
	//
	// AWS documentation:
	//
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-api
	maxBatchSize = 25

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
  --quiet		Disable all output (except for required input)
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
		pterm.Info.Println("Performing dry run. No data will be deleted.")
	}

	if parsedArguments.debug {
		pterm.EnableDebugMessages()
	}

	if parsedArguments.quiet {
		pterm.DisableOutput()
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
		ok := askForConfirmation(reader, tableInfo, parsedArguments.quiet)
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

func askForConfirmation(reader io.RuneReader, tableInfo *dynamodb.DescribeTableOutput, quiet bool) bool {
	// Explicitly enable output before showing the question. The user might have used the --quiet flag. If that is the
	// case the output would not be enabled, the question would not be shown.
	pterm.EnableOutput()
	pterm.Warning.Printf("Do you really want to delete approximately %d items from table %s? [Y/n] ", tableInfo.Table.ItemCount, *tableInfo.Table.TableArn)
	if quiet {
		pterm.DisableOutput()
	}

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
		pterm.Info.Println("You selected 'n'. Aborting truncate operation.")
		return false
	default:
		pterm.Info.Println("Neither 'Y' nor 'n' selected. Aborting truncate operation.")
		return false
	}
}

func retrieveTableInformation(config configuration) (*dynamodb.DescribeTableOutput, error) {
	pterm.Debug.Printf("Retrieving table information for table %s\n", config.table)

	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(config.table),
	}
	pterm.Debug.Printf("DescribeTableInput: %v\n", prettify(*params))

	output, err := config.db.DescribeTable(context.TODO(), params)
	if err != nil {
		return nil, fmt.Errorf("unable to get table information for table %s: %w", config.table, err)
	}
	pterm.Debug.Printf("DescribeTableOutput: %v\n", prettify(*output))

	return output, nil
}

func truncateTable(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput) error {
	// The following code will create 'n' segments. Each segment will be run in its own Go routine, to allow parallel
	// processing of those segments. Each segment itself will process a chunk of the table in so called pages. This
	// will create some parallelism. To stop processing if an error occurs in any of the segments, a "errgroup" is used.
	// An error in one Go routine will cancel all the other Go routines.
	group, ctx := errgroup.WithContext(ctx)

	spinner, err := pterm.DefaultSpinner.Start(fmt.Sprintf("Truncating table %s", *tableInfo.Table.TableArn))
	if err != nil {
		return err
	}

	segments := 4
	for segment := 0; segment < segments; segment++ {
		pterm.Debug.Printf("Start segment %d of %d\n", segment, segments)

		total := segments
		current := segment
		group.Go(func() error {
			return processSegment(ctx, config, tableInfo, total, current, group)
		})
	}

	err = group.Wait()
	if err != nil {
		return err
	}

	spinner.Success(fmt.Sprintf("Truncated table %s", *tableInfo.Table.TableArn))

	return nil
}

func processSegment(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput, totalSegments int, segment int, g *errgroup.Group) error {
	pterm.Debug.Printf("Start processing segment %d\n", segment)

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
	pterm.Debug.Printf("ScanInput: %v\n", prettify(*params))

	paginator := dynamodb.NewScanPaginator(config.db, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			pterm.Error.Printf("Unable to get next page. Continuing with next page. Cause: %v\n", err)
			continue
		}

		config.stats.addRCU(*page.ConsumedCapacity.CapacityUnits)

		g.Go(func() error {
			return processPage(ctx, config, page)
		})
	}

	pterm.Debug.Printf("Finish processing segment %d\n", segment)
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
	for from = 0; from < total; from += maxBatchSize {
		to += maxBatchSize
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
	batchSize := uint64(len(items))
	var processed uint64

	requests := newDeleteRequests(items)

	// TODO: This is a potential endless loop. There should be a "circuit breaker" if the loop iterates too often.
	// Process the list of 'unprocessed' items until there are no unprocessed items left. There might be circumstances
	// when only a subset of items can be processed with a single 'BatchWriteItem' request. So this is a basic "retry"
	// strategy, which is distinct from the API retry strategy, which takes place when there are HTTP errors etc.
	unprocessed := map[string][]types.WriteRequest{config.table: requests}
	for ok := true; ok; ok = len(unprocessed) > 0 {
		// The dry run should be handled as late as possible to allow as much as possible "real" output to happen before
		// the processing stops. Therefore, the dry run is handled here, right before the call to 'BatchWriteItem' is
		// done and statistics are updated.
		if config.dryRun {
			unprocessed = map[string][]types.WriteRequest{}
			continue
		}

		params := &dynamodb.BatchWriteItemInput{
			RequestItems:           unprocessed,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}

		output, err := config.db.BatchWriteItem(ctx, params)
		if err != nil {
			return fmt.Errorf("unable to send delete requests: %w", err)
		}

		for _, u := range output.ConsumedCapacity {
			config.stats.addWCU(*u.CapacityUnits)
		}

		unprocessed = output.UnprocessedItems
		processed = batchSize - processed - uint64(len(unprocessed))
		config.stats.increaseDeleted(processed)
	}

	return nil
}

// newDeleteRequests creates delete requests from the given DynamoDB items (keys) which then can be used with the
// DynamoDB clients 'BatchWriteItem' method.
func newDeleteRequests(items []map[string]types.AttributeValue) []types.WriteRequest {
	requests := make([]types.WriteRequest, len(items))

	for index, key := range items {
		requests[index] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: key,
			},
		}
	}

	return requests
}

type DynamoDBAPI interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

func prettify(o interface{}) string {
	data, err := json.Marshal(o)
	if err != nil {
		pterm.Error.Println(err)
		return ""
	}
	return string(data)
}
