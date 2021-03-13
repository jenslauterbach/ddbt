package main

import (
	"bytes"
	"context"
	"ddbt/internal"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"io/ioutil"
	"log"
	"testing"
)

const (
	hashKeyName = "uuid"
	tableName   = "TestTable"
)

var testLogger = log.New(ioutil.Discard, "", 0)

type batchWriteItemMock struct {
	DynamoDBAPI

	output    []*dynamodb.BatchWriteItemOutput
	error     error
	callCount int
}

func (mock *batchWriteItemMock) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	defer func(mock *batchWriteItemMock) { mock.callCount++ }(mock)

	if mock.output != nil {
		return mock.output[mock.callCount], mock.error
	} else {
		return nil, mock.error
	}
}

func Test_deleteBatch(t *testing.T) {
	type args struct {
		config configuration
		items  []map[string]types.AttributeValue
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
				items: createRandomItems(t, 25),
				config: configuration{
					db: &batchWriteItemMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: map[string][]types.WriteRequest{}},
						},
					},
					table:  tableName,
					logger: testLogger,
					stats:  &statistics{},
				},
			},
			wantErr:       false,
			wantCallCount: 1,
		},
		{
			name: "unprocessed-items-1",
			args: args{
				items: createRandomItems(t,25),
				config: configuration{
					db: &batchWriteItemMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: createRandomUnprocessedItems(t, tableName, 12)},
							{UnprocessedItems: map[string][]types.WriteRequest{}},
						},
					},
					table:  tableName,
					logger: testLogger,
					stats:  &statistics{},
				},
			},
			wantErr:       false,
			wantCallCount: 2,
		},
		{
			name: "unprocessed-items-2",
			args: args{
				items: createRandomItems(t,25),
				config: configuration{
					db: &batchWriteItemMock{
						output: []*dynamodb.BatchWriteItemOutput{
							{UnprocessedItems: createRandomUnprocessedItems(t, tableName, 12)},
							{UnprocessedItems: createRandomUnprocessedItems(t, tableName, 5)},
							{UnprocessedItems: map[string][]types.WriteRequest{}},
						},
					},
					table:  tableName,
					logger: testLogger,
					stats:  &statistics{},
				},
			},
			wantErr:       false,
			wantCallCount: 3,
		},
		{
			name: "delete-request-fails",
			args: args{
				items: createRandomItems(t,25),
				config: configuration{
					db: &batchWriteItemMock{
						output: nil,
						error:  fmt.Errorf("unit test error: %s", "delete-request-fails"),
					},
					table:  tableName,
					logger: testLogger,
					stats:  &statistics{},
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

			gotCallCount := tt.args.config.db.(*batchWriteItemMock).callCount
			if gotCallCount != tt.wantCallCount {
				t.Errorf("deleteBatch() callCount = %d, wantCallCount: %d", gotCallCount, tt.wantCallCount)
			}
		})
	}
}

func createRandomItems(t *testing.T, count int) []map[string]types.AttributeValue {
	t.Helper()

	var items []map[string]types.AttributeValue

	for i := 0; i < count; i++ {
		items = append(items, map[string]types.AttributeValue{
			hashKeyName: &types.AttributeValueMemberS{Value: internal.RandomString(12)},
		})
	}

	return items
}

func createRandomUnprocessedItems(t *testing.T, table string, count int) map[string][]types.WriteRequest {
	t.Helper()

	var items []types.WriteRequest

	for i := 0; i < count; i++ {
		items = append(items, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: map[string]types.AttributeValue{
					hashKeyName: &types.AttributeValueMemberS{Value: internal.RandomString(12)},
				},
			},
		})
	}

	return map[string][]types.WriteRequest{table: items}
}

func Test_newProjection(t *testing.T) {
	type args struct {
		keys []types.KeySchemaElement
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "one-key",
			args: args{
				keys: []types.KeySchemaElement{
					{AttributeName: aws.String("uuid"), KeyType: types.KeyTypeHash},
				},
			},
			want:    []string{"uuid"},
			wantErr: false,
		},
		{
			name: "two-keys",
			args: args{
				keys: []types.KeySchemaElement{
					{AttributeName: aws.String("uuid"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("created"), KeyType: types.KeyTypeRange},
				},
			},
			want:    []string{"uuid", "created"},
			wantErr: false,
		},
		{
			name: "err",
			args: args{
				keys: []types.KeySchemaElement{
					{AttributeName: aws.String(""), KeyType: ""},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newProjection(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("newProjection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// when an error is expected, no need for further tests.
			if tt.wantErr {
				return
			}

			if got == nil {
				t.Errorf("newProject() returned nil")
				return
			}

			for _, wantName := range tt.want {
				ok := false
				for _, gotName := range got.Names() {
					if gotName == wantName {
						ok = true
					}
				}
				if !ok {
					t.Errorf("newProjection() does not contain key name '%s'", wantName)
					return
				}
			}
		})
	}
}

func Test_askForConfirmation(t *testing.T) {
	// Note: This test fails in GoLand because of the output generated by the function under test. The following defect
	// appears to be related to this issue: https://youtrack.jetbrains.com/issue/GO-7215
	// The text will still work if you run 'go test'.
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"confirm", "Y", true},
		{"y", "y", false},
		{"abort", "n", false},
		{"uppercase-N", "N", false},
		{"word", "test", false},
		{"1", "1", false},
		{"0", "0", false},
	}

	tableInfo := &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{TableArn: aws.String("test"), ItemCount: 1337},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := askForConfirmation(bytes.NewBufferString(tt.input), tableInfo)

			if got != tt.want {
				t.Errorf("askForConfirmation() got = %v, want = %v", got, tt.want)
			}
		})
	}
}
