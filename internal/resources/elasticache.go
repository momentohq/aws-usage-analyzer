package resources

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"log"
	"strconv"
	"strings"
)

type Elasticache struct {
	Client *elasticache.Client
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

func (ec *Elasticache) GetAll() ([]*ResourceSummary, error) {
	var results []*ResourceSummary
	rsp, err := ec.Client.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{
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
				if c.ReplicationGroupId != nil {
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
				}

				// Elasticache does not set ReplicationGroupId consistently on non replicated
				// clusters made via CLI vs UI. So we have to do this to determine proper
				// cluster name
				clusterId := *c.CacheClusterId
				if c.ReplicationGroupId != nil {
					clusterId = *c.ReplicationGroupId
				}

				results = append(results, &ResourceSummary{
					ID:   *c.CacheClusterId,
					Type: AwsElasticacheRedisNode,
					AdditionalData: map[string]string{
						"cluster_id":           clusterId,
						"engine":               *c.Engine,
						"cache_node_type":      *c.CacheNodeType,
						"preferred_az":         *c.PreferredAvailabilityZone,
						"cluster_mode_enabled": strconv.FormatBool(clusterModeEnabled),
					},
					Resource: ec,
				})
			case "memcached":
				for _, cacheNode := range c.CacheNodes {
					results = append(results, &ResourceSummary{
						ID:   *cacheNode.CacheNodeId,
						Type: AwsElasticacheMemcachedNode,
						AdditionalData: map[string]string{
							"cluster_id":           *c.CacheClusterId,
							"engine":               *c.Engine,
							"cache_node_type":      *c.CacheNodeType,
							"preferred_az":         *cacheNode.CustomerAvailabilityZone,
							"cluster_mode_enabled": strconv.FormatBool(false),
						},
						Resource: ec,
					})
				}
			}
		}

		// Paginate till we got all cache clusters
		if rsp.Marker != nil {
			rsp, err = ec.Client.DescribeCacheClusters(context.TODO(), &elasticache.DescribeCacheClustersInput{
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
func (ec *Elasticache) GetMetricTargets(r *ResourceSummary) ResourceMetricTargets {
	if r.Type == "AWS::Elasticache::RedisNode" {
		return ResourceMetricTargets{
			Namespace: "AWS/ElastiCache",
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("CacheClusterId"),
					Value: aws.String(r.ID), // for redis metrics full node id is needed for cluster name :/
				},
				{
					Name: aws.String("CacheNodeId"),
					// for redis metrics cacheNodeID is always hard coded to 0001 :/
					Value: aws.String("0001"),
				},
			},
			Targets: cacheMetricsToGet,
		}
	}
	return ResourceMetricTargets{
		Namespace: "AWS/ElastiCache",
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("CacheClusterId"),
				Value: aws.String(r.AdditionalData["cluster_id"]),
			},
			{
				Name:  aws.String("CacheNodeId"),
				Value: aws.String(r.ID),
			},
		},
		Targets: cacheMetricsToGet,
	}
}
