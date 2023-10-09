package resources

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	cwTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
)

var ddbTableMetricsToGet = map[string][]string{
	"Sum": {
		"ConsumedReadCapacityUnits",
		"ConsumedWriteCapacityUnits",
		"ProvisionedReadCapacityUnits",
		"ProvisionedWriteCapacityUnits",

		"TimeToLiveDeletedItemCount",
	},
}
var ddbGSIMetricsToGet = map[string][]string{
	"Sum": {
		"ConsumedReadCapacityUnits",
		"ConsumedWriteCapacityUnits",
		"ProvisionedReadCapacityUnits",
		"ProvisionedWriteCapacityUnits",
	},
}

type DynamoDb struct {
	Client *dynamodb.Client
}

func (ddb *DynamoDb) GetMetricTargets(r *ResourceSummary) ResourceMetricTargets {
	if r.Type == AwsDynamoDbGsi {
		return ResourceMetricTargets{
			Namespace: "AWS/DynamoDB",
			Dimensions: []cwTypes.Dimension{
				{
					Name:  aws.String("TableName"),
					Value: aws.String(r.ID),
				},
				{
					Name:  aws.String("GlobalSecondaryIndexName"),
					Value: aws.String(r.AdditionalData["gsi_name"]),
				},
			},
			Targets: ddbGSIMetricsToGet,
		}
	}

	return ResourceMetricTargets{
		Namespace: "AWS/DynamoDB",
		Dimensions: []cwTypes.Dimension{
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

		// Fetch info about table
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

		// Calc Main Table Metrics
		ttlEnabled := false
		if dTTLRsp.TimeToLiveDescription.TimeToLiveStatus == "ENABLED" {
			ttlEnabled = true
		}

		avg_item_size := int64(0)
		if *dRsp.Table.ItemCount != 0 {
			avg_item_size = *dRsp.Table.TableSizeBytes / *dRsp.Table.ItemCount
		}

		billingMode := ddbTypes.BillingModeProvisioned
		if dRsp.Table.BillingModeSummary != nil {
			billingMode = dRsp.Table.BillingModeSummary.BillingMode
		}

		tableMetadata := map[string]string{
			"ttl_enabled":         strconv.FormatBool(ttlEnabled),
			"item_count":          strconv.FormatInt(*dRsp.Table.ItemCount, 10),
			"table_size_bytes":    strconv.FormatInt(*dRsp.Table.TableSizeBytes, 10),
			"avg_item_size_bytes": strconv.FormatInt(avg_item_size, 10),
			"billing_mode":        string(billingMode),
		}

		if dRsp.Table.ProvisionedThroughput != nil {
			throughputSum := dRsp.Table.ProvisionedThroughput
			tableMetadata["p_throughput_write_units"] = strconv.FormatInt(*throughputSum.WriteCapacityUnits, 10)
			tableMetadata["p_throughput_read_units"] = strconv.FormatInt(*throughputSum.ReadCapacityUnits, 10)
			tableMetadata["p_throughput_decreases_day"] = strconv.FormatInt(*throughputSum.NumberOfDecreasesToday, 10)
		}

		// Calc  GSI Metrics
		tableMetadata["gsi_count"] = strconv.Itoa(len(dRsp.Table.GlobalSecondaryIndexes))
		var gsiList []*ResourceSummary
		for _, gsi := range dRsp.Table.GlobalSecondaryIndexes {
			gsiMetadata := map[string]string{
				"gsi_name":   *gsi.IndexName,
				"item_count": strconv.FormatInt(*gsi.ItemCount, 10),
				"size_bytes": strconv.FormatInt(*gsi.IndexSizeBytes, 10),
			}

			if gsi.ProvisionedThroughput != nil {
				gsiMetadata["p_throughput_write_units"] = strconv.FormatInt(*gsi.ProvisionedThroughput.WriteCapacityUnits, 10)
				gsiMetadata["p_throughput_read_units"] = strconv.FormatInt(*gsi.ProvisionedThroughput.ReadCapacityUnits, 10)
				gsiMetadata["p_throughput_decreases_day"] = strconv.FormatInt(*gsi.ProvisionedThroughput.NumberOfDecreasesToday, 10)
			}
			if gsi.Projection != nil {
				gsiMetadata["projection_type"] = string(gsi.Projection.ProjectionType)
			}

			gsiList = append(gsiList, &ResourceSummary{
				ID:             table,
				Type:           AwsDynamoDbGsi,
				AdditionalData: gsiMetadata,
				Resource:       ddb,
			})
		}

		returnList = append(returnList, &ResourceSummary{
			ID:             table,
			Type:           AwsDynamoDbTable,
			AdditionalData: tableMetadata,
			Resource:       ddb,
		})
		returnList = append(returnList, gsiList...)
	}

	return returnList, nil
}
