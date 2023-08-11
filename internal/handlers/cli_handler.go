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
	ID                 string       `json:"id"`
	ClusterId          string       `json:"cluster_id"`
	Engine             string       `json:"engine"`
	CacheNodeType      string       `json:"cache_node_type"`
	PreferredAZ        string       `json:"preferred_az"`
	ClusterModeEnabled bool         `json:"cluster_mode_enabled"`
	Metrics            []MetricBlob `json:"metrics"`
}

const MaxConcurrency = 3

func (h *Handler) Handle() error {

	fmt.Println("Starting Elasticache Usage Analyzer")
	fmt.Println("-----------------------------------")
	fmt.Println("")

	results, err := getAllElasticacheNodes(h.Ec)
	if err != nil {
		return err
	}
	fmt.Printf("Found %d cache nodes to collect data on.\n\n", len(results))
	fmt.Println("Note: Please be patient if this is going slowly")
	fmt.Println("Speed is limited by cloudwatch:GetMetricData api limits.")
	fmt.Printf("Can increase account limits in AWS Console.\n\n")
	fmt.Printf("Collecting Data on Nodes:\n\n")

	// Init progress bar to show user feedback on script progress
	bar := pb.Simple.Start(len(results))

	// Wait group and concurrency guard to grab node monitoring data with controlled concurrency
	var wg = &sync.WaitGroup{}
	guard := make(chan struct{}, MaxConcurrency) // Guard to limit max concurrency
	wg.Add(len(results))
	for _, clusterNode := range results {
		guard <- struct{}{} // Limits max concurrency to maxGoroutines
		go getNodeMetrics(h.Cw, clusterNode, wg, bar)
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
		{"NodeId", "ClusterID", "Engine", "NodeType", "AzPreference", "ClusterMode", "Metrics"},
	}
	for _, r := range results {
		metrics, err := json.Marshal(r.Metrics)
		if err != nil {
			return err
		}
		data = append(data, []string{
			r.ID,
			r.ClusterId,
			r.Engine,
			r.CacheNodeType,
			r.PreferredAZ,
			strconv.FormatBool(r.ClusterModeEnabled),
			string(metrics),
		})
	}

	err = writer.WriteAll(data)
	if err != nil {
		return err
	}
	return nil
}

func getAllElasticacheNodes(client *elasticache.Client) ([]*CacheNodeSummary, error) {
	var results []*CacheNodeSummary

	fmt.Println("Fetching cluster information:")
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

				results = append(results, &CacheNodeSummary{
					ID:                 *c.CacheClusterId,
					ClusterId:          *c.ReplicationGroupId,
					Engine:             *c.Engine,
					CacheNodeType:      *c.CacheNodeType,
					PreferredAZ:        *c.PreferredAvailabilityZone,
					ClusterModeEnabled: clusterModeEnabled,
				})
			case "memcached":
				for _, cacheNode := range c.CacheNodes {
					results = append(results, &CacheNodeSummary{
						ID:                 *cacheNode.CacheNodeId,
						ClusterId:          *c.CacheClusterId,
						Engine:             *c.Engine,
						CacheNodeType:      *c.CacheNodeType,
						PreferredAZ:        *cacheNode.CustomerAvailabilityZone,
						ClusterModeEnabled: false, // No such thing for memcached
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
func getNodeMetrics(cwClient *cloudwatch.Client, nodeSummary *CacheNodeSummary, group *sync.WaitGroup, bar *pb.ProgressBar) {
	defer group.Done()

	// Calc node and cluster id to use to fetch metrics
	cacheNodeId := nodeSummary.ID
	cacheClusterID := nodeSummary.ClusterId
	if nodeSummary.Engine == "redis" {
		cacheNodeId = "0001"            // for redis metrics cacheNodeID is always hard coded to 0001 :/
		cacheClusterID = nodeSummary.ID // for redis metrics full node id is needed for cluster name :/
	}

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
