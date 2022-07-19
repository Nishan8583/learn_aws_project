package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// references:
// https://dynobase.dev/dynamodb-golang-query-examples/#list-tables
// https://golangexample.com/project-making-unit-test-in-dynamodb/
func main() {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID && region == "us-west-2" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           "http://localhost:8000",
				SigningRegion: "us-west-2",
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		log.Fatal("FATAL could not load local dynamo DB")

	}

	svc := dynamodb.NewFromConfig(cfg)
	log.Println("dyanmo DB instance started")
	tables, err := svc.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Println("INFO table output", tables, err)
		return
	}

	log.Println("Listing tables")
}
