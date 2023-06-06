package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

var metricsToGet = []string{
	"CurrConnections",
	"NewConnections",
	"NetworkBytesIn",
	"NetworkBytesOut",
	"EngineCPUUtilization",
	"CPUUtilization",
	"FreeableMemory",

	"BytesUsedForCache",
	"DatabaseMemoryUsagePercentage",
	"CurrItems",
	"KeysTracked",
	"Evictions",

	"CacheHitRate",
	"DB0AverageTTL",

	"EvalBasedCmds",
	"EvalBasedCmdsLatency",
	"GeoSpatialBasedCmds",
	"GeoSpatialBasedCmdsLatency",
	"GetTypeCmds",
	"GetTypeCmdsLatency",
	"HashBasedCmds",
	"HashBasedCmdsLatency",
	"JsonBasedCmds",
	"JsonBasedCmdsLatency",
	"KeyBasedCmds",
	"KeyBasedCmdsLatency",
	"ListBasedCmds",
	"ListBasedCmdsLatency",
	"PubSubBasedCmds",
	"PubSubBasedCmdsLatency",
	"SetBasedCmds",
	"SetBasedCmdsLatency",
	"SetTypeCmds",
	"SetTypeCmdsLatency",
	"SortedSetBasedCmds",
	"SortedSetBasedCmdsLatency",
	"StringBasedCmds",
	"StringBasedCmdsLatency",
	"StreamBasedCmds",
	"StreamBasedCmdsLatency",
}

type MetricBlob struct {
	Name string `json:"name"`
	//Timestamps []time.Time `json:"timestamps"`
	Values []float64 `json:"values"`
}

type CacheNodeSummary struct {
	ID      string       `json:"id"`
	Metrics []MetricBlob `json:"metrics"`
}

type ClusterSummary struct {
	ID            string             `json:"id"`
	NodeSummary   []CacheNodeSummary `json:"node_summary"`
	Engine        string             `json:"engine"`
	CacheNodeType string             `json:"cache_node_type"`
}

type ClusterNode struct {
	id     string
	engine string
}

func (h *Handler) Handle() error {
	results := map[string]ClusterSummary{}
	var clusterNodes []*ClusterNode

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

		node := &ClusterNode{
			id:     *c.CacheClusterId,
			engine: *c.Engine,
		}
		clusterNodes = append(clusterNodes, node)
		clusterName := deriveClusterName(node)
		results[clusterName] = ClusterSummary{
			ID:            clusterName,
			Engine:        *c.Engine,
			NodeSummary:   []CacheNodeSummary{},
			CacheNodeType: *c.CacheNodeType,
		}
	}
	if err != nil {
		return err
	}

	fmt.Println("Found the following Clusters to collect data on:")
	for _, s := range results {
		fmt.Println(s.ID)
	}

	for _, clusterNode := range clusterNodes {
		summary := results[deriveClusterName(clusterNode)]
		nodeSummary := CacheNodeSummary{
			ID:      clusterNode.id,
			Metrics: []MetricBlob{},
		}
		for _, metric := range metricsToGet {

			data, err := h.Cw.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
				EndTime: aws.Time(time.Now()),
				MetricDataQueries: []types.MetricDataQuery{
					{
						MetricStat: &types.MetricStat{
							Metric: &types.Metric{
								MetricName: aws.String(metric),
								Namespace:  aws.String("AWS/ElastiCache"),
								Dimensions: []types.Dimension{
									{
										Name:  aws.String("CacheClusterId"),
										Value: aws.String(clusterNode.id),
									},
								},
							},
							Period: aws.Int32(60 * 60 * 24 * 30), // 30 day interval
							Stat:   aws.String("Maximum"),
						},
						Id: aws.String("foo"), // doesnt matter grabbing one  metric at a time
					},
				},
				StartTime: aws.Time(time.Now().Add(time.Duration(-30) * 24 * time.Hour)),
			})

			if err != nil {
				return err
			}
			mBlob := MetricBlob{
				Name:   metric,
				Values: data.MetricDataResults[len(data.MetricDataResults)-1].Values,
			}

			nodeSummary.Metrics = append(nodeSummary.Metrics, mBlob)
		}
		summary.NodeSummary = append(summary.NodeSummary, nodeSummary)
		results[deriveClusterName(clusterNode)] = summary
	}
	f, err := os.Create("./results.csv")
	if err != nil {
		return err
	}

	writer := csv.NewWriter(f)

	data := [][]string{
		{"ClusterID", "Engine", "NodeType", "NumNodes", "Metrics"},
	}
	for _, r := range results {
		metrics, err := json.MarshalIndent(r.NodeSummary, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, []string{
			r.ID,
			r.Engine,
			r.CacheNodeType,
			strconv.Itoa(len(r.NodeSummary)),
			string(metrics),
		})
	}

	err = writer.WriteAll(data)
	if err != nil {
		return err
	}
	return nil
}

func deriveClusterName(node *ClusterNode) string {
	switch node.engine {
	case "redis":
		idChunks := strings.Split(node.id, "-")
		suffix := "-" + idChunks[len(idChunks)-2] + "-" + idChunks[len(idChunks)-1]
		clusterName := strings.TrimSuffix(node.id, suffix)
		return clusterName
	default:
		return ""
	}
}
