package resources

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"strconv"
)

var ddbTableMetricsToGet = map[string][]string{
	"Sum": {
		"ConsumedReadCapacityUnits",
		"ConsumedWriteCapacityUnits",

		"TimeToLiveDeletedItemCount",
	},
}

type DynamoDb struct {
	Client *dynamodb.Client
}

func (ddb *DynamoDb) GetMetricTargets(r *ResourceSummary) ResourceMetricTargets {
	return ResourceMetricTargets{
		Namespace: "AWS/DynamoDB",
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("TableName"),
				Value: aws.String(r.ID),
			},
		},
		Targets: ddbTableMetricsToGet,
	}
}

func (ddb *DynamoDb) GetAll() ([]*ResourceSummary, error) {
	var returnList []*ResourceSummary

	listTableRsp, err := ddb.Client.ListTables(context.TODO(), &dynamodb.ListTablesInput{
		Limit: aws.Int32(100),
	})
	if err != nil {
		return nil, err
	}

	tables := listTableRsp.TableNames
	lastTableSeen := listTableRsp.LastEvaluatedTableName
	if lastTableSeen != nil {
		for {
			listTableRsp, err := ddb.Client.ListTables(context.TODO(), &dynamodb.ListTablesInput{
				ExclusiveStartTableName: lastTableSeen,
				Limit:                   aws.Int32(100),
			})
			if err != nil {
				return nil, err
			}
			tables = append(tables, listTableRsp.TableNames...)
			lastTableSeen = listTableRsp.LastEvaluatedTableName
			if lastTableSeen == nil {
				break
			}
		}
	}

	for _, table := range tables {
		dRsp, err := ddb.Client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			return nil, err
		}
		// TODO this is very slow API maybe break into its own step
		dTTLRsp, err := ddb.Client.DescribeTimeToLive(context.TODO(), &dynamodb.DescribeTimeToLiveInput{
			TableName: aws.String(table),
		})
		if err != nil {
			return nil, err
		}
		ttlEnabled := false
		if dTTLRsp.TimeToLiveDescription.TimeToLiveStatus == "ENABLED" {
			ttlEnabled = true
		}

		returnList = append(returnList, &ResourceSummary{
			ID:   table,
			Type: "AWS::DynamoDB::Table",
			AdditionalData: map[string]string{
				"ttl_enabled": strconv.FormatBool(ttlEnabled),
				"item_count":  strconv.FormatInt(*dRsp.Table.ItemCount, 10),
			},
			Resource: ddb,
		})
	}

	return returnList, nil
}
