package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type userInfo struct {
	Name string
	ID   string `dynamodbav:"id"`
}

// references:
// https://dynobase.dev/dynamodb-golang-query-examples/#list-tables
// https://www.digitalocean.com/community/tutorials/how-to-use-contexts-in-go
// https://golangexample.com/project-making-unit-test-in-dynamodb/
// https://medium.com/yemeksepeti-teknoloji/dynamodb-with-aws-sdk-go-v2-part-2-crud-operations-3da68c2f431f
func main() {

	// creating custom resolver to test in local dynamoDB instance
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:8000",
			SigningRegion: "us-west-2",
		}, nil

	})
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		log.Fatal("FATAL could not load local dynamo DB")
	}

	// setting up dynamoDB client
	svc := dynamodb.NewFromConfig(cfg)
	log.Println("dyanmo DB instance started")
	tables, err := svc.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Println("INFO table output", tables, err)
		return
	}

	//createTable(svc)
	//_ = insertValue(svc)
	log.Println(getItem(svc, userInfo{ID: "1c743891-9d33-4fbf-8e44-e5685694da5b"}))

	//log.Println(c)
}

func createTable(svc *dynamodb.Client) {
	tables, err := svc.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("Could not list tables ", err)
	}

	for _, names := range tables.TableNames {
		if names == "test_table_2" {
			log.Println("table already present, skipping table creation")
			return
		}
	}

	// code for creating table
	inputTable := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},

		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName:   aws.String("test_table_2"),
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err = svc.CreateTable(context.TODO(), inputTable)
	if err != nil {
		log.Fatal("ERROR could not create table due to error,", err)
		return
	}
}

func insertValue(svc *dynamodb.Client) userInfo {

	item := userInfo{
		Name: "test_user",
		ID:   uuid.NewString(),
	}

	data, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Println("ERROR could not map item ", err)
		return item
	}
	input := &dynamodb.PutItemInput{
		Item:      data,
		TableName: aws.String("test_table_2"),
	}
	log.Println("inputting", input)
	_, err = svc.PutItem(context.TODO(), input)
	if err != nil {
		log.Fatal("Could not put item", err)
	}
	log.Println("inserted value", input)
	return item
}

func getItem(svc *dynamodb.Client, ui userInfo) (userInfo, error) {

	item := userInfo{}
	keys := map[string]string{
		"id": ui.ID,
	}
	pi, err := attributevalue.MarshalMap(keys)
	if err != nil {
		return item, errors.Wrap(err, "while marshalling keys")
	}

	getItemInput := &dynamodb.GetItemInput{TableName: aws.String("test_table_2"), Key: pi}
	output, err := svc.GetItem(context.TODO(), getItemInput)
	if err != nil {
		return item, errors.Wrap(err, "FATAL could not get data from dynamodb")
	}

	if output.Item == nil {
		return item, errors.New("data not found")
	}

	if err := attributevalue.UnmarshalMap(output.Item, &item); err != nil {
		return item, errors.Wrap(err, "Could not unmarshall dynamoDB output")
	}

	return item, err
}
