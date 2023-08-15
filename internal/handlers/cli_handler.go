package handlers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/momentohq/aws-usage-analyzer/internal/metrics"
	"github.com/momentohq/aws-usage-analyzer/internal/resources"
	"github.com/momentohq/aws-usage-analyzer/internal/util"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

type Handler struct {
	Cw  *cloudwatch.Client
	Ec  *elasticache.Client
	DDB *dynamodb.Client
}

func (h *Handler) Handle() error {
	util.MultiPrintln([]string{
		"Starting AWS Usage Collector",
		"-----------------------------------",
	})

	fmt.Println("Looking for Elasticache Nodes to analyze:")
	ec := &resources.Elasticache{Client: h.Ec}
	elasticacheNodes, err := ec.GetAll()
	if err != nil {
		return err
	}
	fmt.Printf("Found %d cache nodes to collect data on.\n\n", len(elasticacheNodes))

	fmt.Println("Looking for DDB tables to analyze:")
	ddb := &resources.DynamoDb{Client: h.DDB}
	ddbTables, err := ddb.GetAll()
	if err != nil {
		return err
	}
	fmt.Printf("Found %d ddb tables to collect data on.\n\n", len(ddbTables))

	util.MultiPrintln([]string{
		"Starting metrics collection on resources",
		"Note: Please be patient if this is going slowly",
		"Can increase account limits in AWS Console for ",
		"CloudWatch GetMetrics API.\n\n",
		"Collecting Data on Resources:",
	})

	resourcesToFetchMetricsOn := append(ddbTables, elasticacheNodes...)

	metricFetcher := metrics.ResourceMetricFetcher{CW: h.Cw}
	metricFetcher.GetMetricsForResources(resourcesToFetchMetricsOn)
	// Write out resource data to csv
	err = writeOutResults(resourcesToFetchMetricsOn)
	if err != nil {
		return err
	}

	fmt.Println("")
	fmt.Println("Finished collecting data! Please send results.csv to Momento for analysis")

	return nil
}

func writeOutResults(results []*resources.ResourceSummary) error {
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
