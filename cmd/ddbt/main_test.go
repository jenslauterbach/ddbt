package main

import (
	"context"
	"ddbt/internal"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"testing"
)

const (
	hashKeyName = "uuid"
	tableName   = "TestTable"
)

type batchWriteItemWithContextMock struct {
	dynamodbiface.DynamoDBAPI

	output    []*dynamodb.BatchWriteItemOutput
	error     error
	callCount int
}

func (mock *batchWriteItemWithContextMock) BatchWriteItemWithContext(aws.Context, *dynamodb.BatchWriteItemInput, ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	defer func(mock *batchWriteItemWithContextMock) { mock.callCount++ }(mock)

	if mock.output != nil {
		return mock.output[mock.callCount], mock.error
	} else {
		return nil, mock.error
	}
}

func Test_deleteBatch(t *testing.T) {
	type args struct {
		config configuration
		items  []map[string]*dynamodb.AttributeValue
	}
	tests := []struct {
		name          string
		args          args
		wantErr       bool
		wantCallCount int
	}{
		{
			name: "ok",
			args: args{
				items: createRandomItems(25),
				config: configuration{
					db: &batchWriteItemWithContextMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: map[string][]*dynamodb.WriteRequest{}},
						},
					},
					table:      tableName,
					maxRetries: 10,
				},
			},
			wantErr:       false,
			wantCallCount: 1,
		},
		{
			name: "unprocessed-items-1",
			args: args{
				items: createRandomItems(25),
				config: configuration{
					db: &batchWriteItemWithContextMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 12)},
							{UnprocessedItems: map[string][]*dynamodb.WriteRequest{}},
						},
					},
					table:      tableName,
					maxRetries: 10,
				},
			},
			wantErr:       false,
			wantCallCount: 2,
		},
		{
			name: "unprocessed-items-2",
			args: args{
				items: createRandomItems(25),
				config: configuration{
					db: &batchWriteItemWithContextMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 12)},
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 5)},
							{UnprocessedItems: map[string][]*dynamodb.WriteRequest{}},
						},
					},
					table:      tableName,
					maxRetries: 10,
				},
			},
			wantErr:       false,
			wantCallCount: 3,
		},
		{
			name: "max-retries-hit",
			args: args{
				items: createRandomItems(25),
				config: configuration{
					db: &batchWriteItemWithContextMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 12)},
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 5)},
							{UnprocessedItems: createRandomUnprocessedItems(tableName, 7)},
							{UnprocessedItems: map[string][]*dynamodb.WriteRequest{}},
						},
					},
					table:      tableName,
					maxRetries: 1,
				},
			},
			wantErr:       false,
			wantCallCount: 2,
		},
		{
			name: "delete-request-fails",
			args: args{
				items: createRandomItems(25),
				config: configuration{
					db: &batchWriteItemWithContextMock{
						output: nil,
						error:  fmt.Errorf("unit test error: %s", "delete-request-fails"),
					},
					table:      tableName,
					maxRetries: 1,
				},
			},
			wantErr:       true,
			wantCallCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := deleteBatch(context.Background(), tt.args.config, tt.args.items); (err != nil) != tt.wantErr {
				t.Errorf("deleteBatch() error = %v, wantErr %v", err, tt.wantErr)
			}

			gotCallCount := tt.args.config.db.(*batchWriteItemWithContextMock).callCount
			if gotCallCount != tt.wantCallCount {
				t.Errorf("deleteBatch() callCount = %d, wantCallCount: %d", gotCallCount, tt.wantCallCount)
			}
		})
	}
}

func createRandomItems(count int) []map[string]*dynamodb.AttributeValue {
	var items []map[string]*dynamodb.AttributeValue

	for i := 0; i < count; i++ {
		items = append(items, map[string]*dynamodb.AttributeValue{
			hashKeyName: {S: aws.String(internal.RandomString(12))},
		})
	}

	return items
}

func createRandomUnprocessedItems(table string, count int) map[string][]*dynamodb.WriteRequest {
	var items []*dynamodb.WriteRequest

	for i := 0; i < count; i++ {
		items = append(items, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					hashKeyName: {
						S: aws.String(internal.RandomString(12)),
					},
				},
			},
		})
	}

	return map[string][]*dynamodb.WriteRequest{table: items}
}
