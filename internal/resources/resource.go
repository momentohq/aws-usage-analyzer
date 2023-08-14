package resources

import "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

type MetricBlob struct {
	Name   string    `json:"name"`
	Values []float64 `json:"values"`
}

type ResourceSummary struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
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
