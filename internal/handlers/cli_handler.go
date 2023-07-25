package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/cheggaaa/pb/v3"
)

type Handler struct {
	Cw *cloudwatch.Client
	Ec *elasticache.Client
}

var metricsToGet = map[string][]string{
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

		"EvalBasedCmdsLatency",
		"GeoSpatialBasedCmdsLatency",
		"GetTypeCmdsLatency",
		"HashBasedCmdsLatency",
		"JsonBasedCmdsLatency",
		"KeyBasedCmdsLatency",
		"ListBasedCmdsLatency",
		"PubSubBasedCmdsLatency",
		"SetBasedCmdsLatency",
		"SetTypeCmdsLatency",
		"SortedSetBasedCmdsLatency",
		"StringBasedCmdsLatency",
		"StreamBasedCmdsLatency",
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

		"EvalBasedCmdsLatency",
		"GeoSpatialBasedCmdsLatency",
		"GetTypeCmdsLatency",
		"HashBasedCmdsLatency",
		"JsonBasedCmdsLatency",
		"KeyBasedCmdsLatency",
		"ListBasedCmdsLatency",
		"PubSubBasedCmdsLatency",
		"SetBasedCmdsLatency",
		"SetTypeCmdsLatency",
		"SortedSetBasedCmdsLatency",
		"StringBasedCmdsLatency",
		"StreamBasedCmdsLatency",
	},
}

type MetricBlob struct {
	Name   string    `json:"name"`
	Values []float64 `json:"values"`
}

type CacheNodeSummary struct {
	ID            string       `json:"id"`
	ClusterId     string       `json:"cluster_id"`
	Metrics       []MetricBlob `json:"metrics"`
	Engine        string       `json:"engine"`
	CacheNodeType string       `json:"cache_node_type"`
}

const MaxConcurrency = 3

func (h *Handler) Handle() error {

	fmt.Println("Starting Elasticache Usage Analyzer")
	fmt.Println("-----------------------------------")
	fmt.Println("")

	results, err := getRedisNodes(h.Ec)
	if err != nil {
		return err
	}
	fmt.Printf("Found %d redis nodes to collect data on.\n\n", len(results))
	fmt.Println("Note: Please be patient if this is going slowly")
	fmt.Println("Speed is limited by cloudwatch:GetMetricData api limits.")
	fmt.Println("Can increase account limits in AWS Console.\n")
	fmt.Println("Collecting Data on Nodes:\n")

	// Init progress bar to show user feedback on script progress
	bar := pb.Simple.Start(len(results))

	// Wait group and concurrency guard to grab node monitoring data with controlled concurrency
	var wg = &sync.WaitGroup{}
	guard := make(chan struct{}, MaxConcurrency) // Guard to limit max concurrency
	wg.Add(len(results))
	for _, clusterNode := range results {
		guard <- struct{}{} // Limits max concurrency to maxGoroutines
		go getNodeMetrics(h.Cw, clusterNode.ID, clusterNode, wg, bar)
		<-guard
	}

	// Block on tasks finishing
	wg.Wait()
	bar.Finish() // complete loading bar for user

	// Write out results to csv
	err = writeOutResults(results)
	if err != nil {
		return err
	}

	fmt.Println("")
	fmt.Println("Finished collecting data! Please send results.csv to Momento for analysis")

	return nil
}

func writeOutResults(results []*CacheNodeSummary) error {
	f, err := os.Create("./results.csv")
	if err != nil {
		return err
	}

	writer := csv.NewWriter(f)

	data := [][]string{
		{"NodeId", "ClusterID", "Engine", "NodeType", "Metrics"},
	}
	for _, r := range results {
		metrics, err := json.MarshalIndent(r.Metrics, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, []string{
			r.ID,
			r.ClusterId,
			r.Engine,
			r.CacheNodeType,
			string(metrics),
		})
	}

	err = writer.WriteAll(data)
	if err != nil {
		return err
	}
	return nil
}

func getRedisNodes(client *elasticache.Client) ([]*CacheNodeSummary, error) {
	var results []*CacheNodeSummary

	fmt.Println("Fetching cluster information:")
	rsp, err := client.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{})
	if err != nil {
		return nil, err
	}

	for {
		for _, c := range rsp.CacheClusters {
			if *c.Engine != "redis" {
				fmt.Printf(
					"ignoring unsupported cluster ignoring type:%s name:%s \n",
					*c.Engine,
					*c.CacheClusterId,
				)
				continue
			}

			results = append(results, &CacheNodeSummary{
				ID:            *c.CacheClusterId,
				ClusterId:     *c.ReplicationGroupId,
				Engine:        *c.Engine,
				CacheNodeType: *c.CacheNodeType,
			})

		}
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
func getNodeMetrics(cwClient *cloudwatch.Client, nodeId string, results *CacheNodeSummary, group *sync.WaitGroup, bar *pb.ProgressBar) {
	defer group.Done()
	for statType, metrics := range metricsToGet {
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
								Value: aws.String(nodeId),
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
				mBlob := MetricBlob{
					Name:   *metric.Id,
					Values: metric.Values,
				}
				results.Metrics = append(results.Metrics, mBlob)
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
