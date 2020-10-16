package main

import (
	"ddbt/internal"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"os"
)

const (
	batchSize = 25
)

func main() {
	err := run()
	if err != nil {
		fmt.Printf("Error %v", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func run() error {
	config, err := newConfig()
	if err != nil {
		return err
	}

	items := createItems(config.count)
	insertItems(config, items)

	return nil
}

func createItems(count int) []string {
	var items []string

	for i := 0; i < count; i++ {
		items = append(items, internal.RandomString(12))
	}

	return items
}

func insertItems(config configuration, items []string) {
	var total = len(items)
	var from int
	var to int
	for from = 0; from < total; from += batchSize {
		to += batchSize
		if to > total {
			to = total
		}

		err := putBatch(config, items[from:to])
		if err != nil {
			fmt.Printf("unable to insert batch: %v\n", err)
		}
		fmt.Printf("%d/%d\n", to, total)
	}
}

func putBatch(config configuration, items []string) error {
	requests := make([]*dynamodb.WriteRequest, len(items))

	for index, item := range items {
		requests[index] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"Name": {S: aws.String(item)},
				},
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			config.table: requests,
		},
	}

	_, err := config.db.BatchWriteItem(input)
	if err != nil {
		return fmt.Errorf("unable to write items: %w", err)
	}

	return nil
}

type configuration struct {
	db    dynamodbiface.DynamoDBAPI
	table string
	count int
}

func newConfig() (configuration, error) {
	region := flag.String("region", "localhost", "AWS region to use")
	endpoint := flag.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	table := flag.String("table-name", "TestTable", "name of the DynamoDB table to truncate")
	count := flag.Int("count", 1_000, "number of items to insert")
	flag.Parse()

	awsConfig := &aws.Config{}

	if *region != "" {
		awsConfig.Region = region
	}

	if *endpoint != "" {
		resolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			return endpoints.ResolvedEndpoint{
				URL:           *endpoint,
				SigningRegion: region,
			}, nil
		}

		awsConfig.EndpointResolver = endpoints.ResolverFunc(resolver)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return configuration{}, fmt.Errorf("unable to create new session: %w", err)
	}

	db := dynamodb.New(sess)

	return configuration{
		db:    db,
		table: *table,
		count: *count,
	}, nil
}
