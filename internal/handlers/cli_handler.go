package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/cheggaaa/pb/v3"
)

type Handler struct {
	Cw  *cloudwatch.Client
	Ec  *elasticache.Client
	DDB *dynamodb.Client
}

var ddbTableMetricsToGet = map[string][]string{
	"Sum": {
		"ConsumedReadCapacityUnits",
		"ConsumedWriteCapacityUnits",

		"TimeToLiveDeletedItemCount",
	},
}

var cacheMetricsToGet = map[string][]string{
	"Sum": {
		"NetworkBytesIn",
		"NetworkBytesOut",

		"GeoSpatialBasedCmds",
		"EvalBasedCmds",
		"GetTypeCmds",
		"HashBasedCmds",

		"JsonBasedCmds",
		"KeyBasedCmds",
		"ListBasedCmds",
		"SetBasedCmds",
		"SetTypeCmds",
		"StringBasedCmds",
		"PubSubBasedCmds",
		"SortedSetBasedCmds",
		"StreamBasedCmds",
	},
	"Average": {
		"DB0AverageTTL",
	},
	"Maximum": {
		"CurrConnections",
		"NewConnections",

		"EngineCPUUtilization",
		"CPUUtilization",
		"FreeableMemory",

		"BytesUsedForCache",
		"DatabaseMemoryUsagePercentage",
		"CurrItems",
		"KeysTracked",
		"Evictions",

		"CacheHitRate",
	},
}

type MetricBlob struct {
	Name   string    `json:"name"`
	Values []float64 `json:"values"`
}

type ResourceSummary struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	AdditionalData map[string]string `json:"additional_data"`
	Metrics        []MetricBlob      `json:"metrics"`
}

type CacheNodeSummary struct {
	ID                 string       `json:"id"`
	ClusterId          string       `json:"cluster_id"`
	Engine             string       `json:"engine"`
	CacheNodeType      string       `json:"cache_node_type"`
	PreferredAZ        string       `json:"preferred_az"`
	ClusterModeEnabled bool         `json:"cluster_mode_enabled"`
	Metrics            []MetricBlob `json:"metrics"`
}

type DdbTableSummary struct {
	ID         string       `json:"id"`
	TTLEnabled bool         `json:"cluster_mode_enabled"`
	ItemCount  int64        `json:"item_count"`
	Metrics    []MetricBlob `json:"metrics"`
}

const MaxConcurrency = 3

func (h *Handler) Handle() error {
	fmt.Println("Starting AWS Usage Collector")
	fmt.Println("-----------------------------------")
	fmt.Println("")

	fmt.Println("Looking for Elasticache Nodes to analyze:")
	elasticacheNodes, err := getAllElasticacheNodes(h.Ec)
	if err != nil {
		return err
	}
	fmt.Printf("Found %d cache nodes to collect data on.\n\n", len(elasticacheNodes))

	fmt.Println("Looking for DDB tables to analyze:")
	ddbTables, err := getAllDDBTables(h.DDB)
	if err != nil {
		return err
	}
	fmt.Printf("Found %d ddb tables to collect data on.\n\n", len(ddbTables))

	fmt.Println("Starting metrics collection on resources")
	fmt.Println("Note: Please be patient if this is going slowly")
	fmt.Println("Speed is limited by cloudwatch:GetMetricData api limits.")
	fmt.Printf("Can increase account limits in AWS Console.\n\n")
	fmt.Printf("Collecting Data on Resources:\n\n")

	resourcesToFetchMetricsOn := append(ddbTables, elasticacheNodes...)

	// Init progress bar to show user feedback on script progress
	bar := pb.Simple.Start(len(resourcesToFetchMetricsOn))

	// Wait group and concurrency guard to grab node monitoring data with controlled concurrency
	var wg = &sync.WaitGroup{}
	guard := make(chan struct{}, MaxConcurrency) // Guard to limit max concurrency
	wg.Add(len(resourcesToFetchMetricsOn))

	for _, resource := range resourcesToFetchMetricsOn {
		guard <- struct{}{} // Limits max concurrency to maxGoroutines
		switch resource.Type {
		case "AWS::Elasticache::RedisNode":
			fallthrough
		case "AWS::Elasticache::MemcachedNode":
			go getElasticacheNodeMetrics(h.Cw, resource, wg, bar)
		case "AWS::DynamoDB::Table":
			go getDDBTableMetrics(h.Cw, resource, wg, bar)

		}
		<-guard
	}

	// Block on tasks finishing
	wg.Wait()
	bar.Finish() // complete loading bar for user

	// Write out resource data to csv
	err = writeOutResults(resourcesToFetchMetricsOn)
	if err != nil {
		return err
	}

	fmt.Println("")
	fmt.Println("Finished collecting data! Please send results.csv to Momento for analysis")

	return nil
}

func getAllDDBTables(ddbClient *dynamodb.Client) ([]*ResourceSummary, error) {
	var returnList []*ResourceSummary

	listTableRsp, err := ddbClient.ListTables(context.TODO(), &dynamodb.ListTablesInput{
		Limit: aws.Int32(100),
	})
	if err != nil {
		return nil, err
	}

	tables := listTableRsp.TableNames
	lastTableSeen := listTableRsp.LastEvaluatedTableName
	if lastTableSeen != nil {
		for {
			listTableRsp, err := ddbClient.ListTables(context.TODO(), &dynamodb.ListTablesInput{
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
		dRsp, err := ddbClient.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			return nil, err
		}
		// TODO this is very slow API maybe break into its own step
		dTTLRsp, err := ddbClient.DescribeTimeToLive(context.TODO(), &dynamodb.DescribeTimeToLiveInput{
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
		})
	}

	return returnList, nil
}

func writeOutResults(results []*ResourceSummary) error {
	f, err := os.Create("./results.csv")
	if err != nil {
		return err
	}

	writer := csv.NewWriter(f)

	data := [][]string{
		{"ResourceId", "Type", "AdditionalData", "Metrics"},
	}
	for _, r := range results {
		metrics, err := json.Marshal(r.Metrics)
		if err != nil {
			return err
		}
		additionalData, err := json.Marshal(r.AdditionalData)
		if err != nil {
			return err
		}
		data = append(data, []string{
			r.ID,
			r.Type,
			string(additionalData),
			string(metrics),
		})
	}

	err = writer.WriteAll(data)
	if err != nil {
		return err
	}
	return nil
}

func getAllElasticacheNodes(client *elasticache.Client) ([]*ResourceSummary, error) {
	var results []*ResourceSummary
	rsp, err := client.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	for {
		for _, c := range rsp.CacheClusters {
			// Elasticache has different API responses depending on engine type need handle both :/
			switch *c.Engine {
			case "redis":
				// Determine if redis node is in cluster mode or not
				clusterModeEnabled := false
				if len(
					strings.Split(
						strings.TrimPrefix(*c.CacheClusterId, *c.ReplicationGroupId+"-"),
						"-",
					),
					// If is in redis cluster mode will have suffix like 002-002 vs just 002
					// This seems to be the best way to determine this from this API right
					// now :(
				) == 2 {
					clusterModeEnabled = true
				}

				results = append(results, &ResourceSummary{
					ID:   *c.CacheClusterId,
					Type: "AWS::Elasticache::RedisNode",
					AdditionalData: map[string]string{
						"cluster_id":           *c.ReplicationGroupId,
						"engine":               *c.Engine,
						"cache_node_type":      *c.CacheNodeType,
						"preferred_az":         *c.PreferredAvailabilityZone,
						"cluster_mode_enabled": strconv.FormatBool(clusterModeEnabled),
					},
				})
			case "memcached":
				for _, cacheNode := range c.CacheNodes {
					results = append(results, &ResourceSummary{
						ID:   *cacheNode.CacheNodeId,
						Type: "AWS::Elasticache::MemcachedNode",
						AdditionalData: map[string]string{
							"cluster_id":           *c.CacheClusterId,
							"engine":               *c.Engine,
							"cache_node_type":      *c.CacheNodeType,
							"preferred_az":         *cacheNode.CustomerAvailabilityZone,
							"cluster_mode_enabled": strconv.FormatBool(false),
						},
					})
				}
			}
		}

		// Paginate till we got all cache clusters
		if rsp.Marker != nil {
			rsp, err = client.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{
				Marker: rsp.Marker,
			})
			if err != nil {
				log.Printf("error grabbing cache nodes err=%+v\n", err)
			}
		} else {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}
func getDDBTableMetrics(cwClient *cloudwatch.Client, ddbTableSummary *ResourceSummary, group *sync.WaitGroup, bar *pb.ProgressBar) {
	defer group.Done()

	for statType, metrics := range ddbTableMetricsToGet {
		var metricsToGrab []types.MetricDataQuery
		for _, metric := range metrics {
			metricsToGrab = append(metricsToGrab, types.MetricDataQuery{
				MetricStat: &types.MetricStat{
					Metric: &types.Metric{
						MetricName: aws.String(metric),
						Namespace:  aws.String("AWS/DynamoDB"),
						Dimensions: []types.Dimension{
							{
								Name:  aws.String("TableName"),
								Value: aws.String(ddbTableSummary.ID),
							},
						},
					},
					Period: aws.Int32(60 * 60 * 24), // 1 day interval
					Stat:   aws.String(statType),
				},
				Id: aws.String(strings.ToLower(metric)),
			})
		}
		startTime := aws.Time(time.Now().Add(time.Duration(-30) * 24 * time.Hour)) // 30 Days ago
		endTime := aws.Time(time.Now())

		data, err := cwClient.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
			EndTime:           endTime,
			MetricDataQueries: metricsToGrab,
			StartTime:         startTime,
		})
		if err != nil {
			log.Printf("error grabbing cw data err=%+v\n", err)
			return
		}

		for {
			for _, metric := range data.MetricDataResults {
				ddbTableSummary.Metrics = append(ddbTableSummary.Metrics, MetricBlob{
					Name:   *metric.Id,
					Values: metric.Values,
				})
			}
			if data.NextToken != nil {
				data, err = cwClient.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
					EndTime:           endTime,
					MetricDataQueries: metricsToGrab,
					StartTime:         startTime,
					NextToken:         data.NextToken,
				})
				if err != nil {
					log.Printf("here error grabbing cw data err=%+v\n", err)
					return
				}
			} else {
				break
			}
		}

	}
	bar.Increment()
}

func getElasticacheNodeMetrics(cwClient *cloudwatch.Client, nodeSummary *ResourceSummary, group *sync.WaitGroup, bar *pb.ProgressBar) {
	defer group.Done()

	// Calc node and cluster id to use to fetch metrics
	cacheNodeId := nodeSummary.ID
	cacheClusterID := nodeSummary.AdditionalData["cluster_id"]
	if nodeSummary.AdditionalData["engine"] == "redis" {
		cacheNodeId = "0001"            // for redis metrics cacheNodeID is always hard coded to 0001 :/
		cacheClusterID = nodeSummary.ID // for redis metrics full node id is needed for cluster name :/
	}

	for statType, metrics := range cacheMetricsToGet {
		var metricsToGrab []types.MetricDataQuery
		for _, metric := range metrics {
			metricsToGrab = append(metricsToGrab, types.MetricDataQuery{
				MetricStat: &types.MetricStat{
					Metric: &types.Metric{
						MetricName: aws.String(metric),
						Namespace:  aws.String("AWS/ElastiCache"),
						Dimensions: []types.Dimension{
							{
								Name:  aws.String("CacheClusterId"),
								Value: aws.String(cacheClusterID),
							},
							{
								Name:  aws.String("CacheNodeId"),
								Value: aws.String(cacheNodeId),
							},
						},
					},
					Period: aws.Int32(60 * 60 * 24), // 1 day interval
					Stat:   aws.String(statType),
				},
				Id: aws.String(strings.ToLower(metric)),
			})
		}
		startTime := aws.Time(time.Now().Add(time.Duration(-30) * 24 * time.Hour)) // 30 Days ago
		endTime := aws.Time(time.Now())

		data, err := cwClient.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
			EndTime:           endTime,
			MetricDataQueries: metricsToGrab,
			StartTime:         startTime,
		})
		if err != nil {
			log.Printf("error grabbing cw data err=%+v\n", err)
			return
		}

		for {
			for _, metric := range data.MetricDataResults {
				nodeSummary.Metrics = append(nodeSummary.Metrics, MetricBlob{
					Name:   *metric.Id,
					Values: metric.Values,
				})
			}
			if data.NextToken != nil {
				data, err = cwClient.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
					EndTime:           endTime,
					MetricDataQueries: metricsToGrab,
					StartTime:         startTime,
					NextToken:         data.NextToken,
				})
				if err != nil {
					log.Printf("error grabbing cw data err=%+v\n", err)
					return
				}
			} else {
				break
			}
		}

	}
	bar.Increment()
}
