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
)

var (
	errTableMissing = errors.New("error: no table name provided")
)

func main() {
	err := run(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func run(args []string) error {
	config, err := newConfig(args)
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
	maxRetries int
}

func newConfig(args []string) (configuration, error) {
	var flags flag.FlagSet
	flags.Init("flags", flag.ExitOnError)
	region := flags.String("region", "", "AWS region to use")
	endpoint := flags.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	err := flags.Parse(args)
	if err != nil {
		return configuration{}, err
	}

	table := flags.Arg(0)
	if table == "" {
		return configuration{}, errTableMissing
	}

	awsConfig := &aws.Config{}

	if *region != "" {
		awsConfig.Region = region
	}

	if *endpoint != "" {
		resolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.DynamodbServiceID {
				return endpoints.ResolvedEndpoint{
					URL:           *endpoint,
					SigningRegion: region,
				}, nil
			}

			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}

		awsConfig.EndpointResolver = endpoints.ResolverFunc(resolver)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return configuration{}, fmt.Errorf("unable to create new session: %w", err)
	}

	db := dynamodb.New(sess)

	return configuration{
		table:      table,
		db:         db,
		maxRetries: defaultMaxRetries,
	}, nil
}

func retrieveTableInformation(config configuration) (*dynamodb.DescribeTableOutput, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(config.table),
	}

	output, err := config.db.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("unable to get table information for table %s: %w", config.table, err)
	}

	return output, nil
}

func truncateTable(ctx context.Context, config configuration, tableInfo *dynamodb.DescribeTableOutput) error {
	g, ctx := errgroup.WithContext(ctx)

	segments := 4
	for segment := 0; segment < segments; segment++ {
		total := segments
		current := segment
		g.Go(func() error {
			return processSegment(config, tableInfo, total, current, ctx, g)
		})
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func processSegment(config configuration, tableInfo *dynamodb.DescribeTableOutput, totalSegments int, segment int, ctx context.Context, g *errgroup.Group) error {
	expr, err := newProjection(tableInfo)
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

	err = config.db.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		g.Go(func() error {
			return processPage(ctx, config, page)
		})
		return true
	})
	if err != nil {
		return err
	}

	return nil
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
	requests := make([]*dynamodb.WriteRequest, len(items))

	for index, key := range items {
		requests[index] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: key,
			},
		}
	}

	retry := 0
	unprocessed := map[string][]*dynamodb.WriteRequest{config.table: requests}
	for ok := true; ok; ok = len(unprocessed) > 0 {
		if retry > config.maxRetries {
			break
		}

		if retry > 0 {
			sleepDuration := time.Duration(math.Pow(backoffBase, float64(retry))) * time.Second
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

func newProjection(tableInfo *dynamodb.DescribeTableOutput) (*expression.Expression, error) {
	// There is at least one key in the table. This one will be added by default. If there is a second key, it is added
	// to the projection as well.
	keys := tableInfo.Table.KeySchema
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
