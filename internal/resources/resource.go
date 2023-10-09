package resources

import "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

type MetricBlob struct {
	Name   string    `json:"name"`
	Values []float64 `json:"values"`
}

type ResourceType string

const (
	AwsElasticacheRedisNode     ResourceType = "AWS::Elasticache::RedisNode"
	AwsElasticacheMemcachedNode ResourceType = "AWS::Elasticache::MemcachedNode"
	AwsDynamoDbTable                         = "AWS::DynamoDB::Table"
	AwsDynamoDbGsi                           = "AWS::DynamoDB::GSI"
)

type ResourceSummary struct {
	ID             string            `json:"id"`
	Type           ResourceType      `json:"type"`
	AdditionalData map[string]string `json:"additional_data"`
	Metrics        []MetricBlob      `json:"metrics"`
	Resource       Resource
}

type ResourceMetricTargets struct {
	Namespace  string
	Dimensions []types.Dimension
	Targets    map[string][]string
}

type Resource interface {
	GetAll() ([]*ResourceSummary, error)
	GetMetricTargets(r *ResourceSummary) ResourceMetricTargets
}
