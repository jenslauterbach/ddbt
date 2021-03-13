package main

import (
	"context"
	"ddbt/internal"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	conf, err := newConfig()
	if err != nil {
		return err
	}

	items := createItems(conf.count)
	insertItems(conf, items)

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
	requests := make([]types.WriteRequest, len(items))

	for index, item := range items {
		requests[index] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"Name": &types.AttributeValueMemberS{Value: item},
				},
			},
		}
	}

	params := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			config.table: requests,
		},
	}

	_, err := config.db.BatchWriteItem(context.TODO(), params)
	if err != nil {
		return fmt.Errorf("unable to write items: %w", err)
	}

	return nil
}

type configuration struct {
	db    *dynamodb.Client
	table string
	count int
}

func newConfig() (configuration, error) {
	region := flag.String("region", "localhost", "AWS region to use")
	endpoint := flag.String("endpoint-url", "", "url of the DynamoDB endpoint to use")
	table := flag.String("table-name", "TestTable", "name of the DynamoDB table to truncate")
	count := flag.Int("count", 1_000, "number of items to insert")
	flag.Parse()

	var options []func(*config.LoadOptions) error

	if *region != "" {
		options = append(options, config.WithRegion(*region))
	}

	if *endpoint != "" {
		resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == dynamodb.ServiceID {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           *endpoint,
					SigningRegion: region,
				}, nil
			}

			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

		options = append(options, config.WithEndpointResolver(resolver))
	}

	c, _ := config.LoadDefaultConfig(context.TODO(), options...)

	return configuration{
		db:    dynamodb.NewFromConfig(c),
		table: *table,
		count: *count,
	}, nil
}
