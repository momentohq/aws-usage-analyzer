package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
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

func (h *Handler) Handle() error {
	results := []*CacheNodeSummary{}

	rsp, err := h.Ec.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{})

	if err != nil {
		return err
	}

	for _, c := range rsp.CacheClusters {
		if *c.Engine != "redis" {
			fmt.Printf(
				"ignoring unsupported cluster ignoring type:%s name:%s \n",
				*c.Engine,
				*c.CacheClusterId,
			)
			continue
		}

		clusterName := deriveClusterName(*c.CacheClusterId, *c.Engine)
		results = append(results, &CacheNodeSummary{
			ID:            *c.CacheClusterId,
			ClusterId:     clusterName,
			Engine:        *c.Engine,
			CacheNodeType: *c.CacheNodeType,
		})
	}
	if err != nil {
		return err
	}

	fmt.Println("Found the following Clusters to collect data on:")
	for _, s := range results {
		fmt.Println(s.ID)
	}

	for _, clusterNode := range results {
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
									Value: aws.String(clusterNode.ID),
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

			data, err := h.Cw.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
				EndTime:           endTime,
				MetricDataQueries: metricsToGrab,
				StartTime:         startTime,
			})
			if err != nil {
				return err
			}

			for {
				for _, metric := range data.MetricDataResults {
					mBlob := MetricBlob{
						Name:   *metric.Id,
						Values: metric.Values,
					}
					clusterNode.Metrics = append(clusterNode.Metrics, mBlob)
				}
				if data.NextToken != nil {
					data, err = h.Cw.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
						EndTime:           endTime,
						MetricDataQueries: metricsToGrab,
						StartTime:         startTime,
						NextToken:         data.NextToken,
					})
					if err != nil {
						return err
					}
				} else {
					break
				}
			}

		}
	}
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

func deriveClusterName(id string, engine string) string {
	switch engine {
	case "redis":
		idChunks := strings.Split(id, "-")
		suffix := "-" + idChunks[len(idChunks)-2] + "-" + idChunks[len(idChunks)-1]
		clusterName := strings.TrimSuffix(id, suffix)
		return clusterName
	default:
		return ""
	}
}
