package metrics

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/cheggaaa/pb/v3"
	"github.com/momentohq/aws-usage-analyzer/internal/resources"
	"log"
	"strings"
	"sync"
	"time"
)

const MaxConcurrency = 3

type ResourceMetricFetcher struct {
	CW *cloudwatch.Client
}

func (c *ResourceMetricFetcher) GetMetricsForResources(resources []*resources.ResourceSummary) {
	// Init progress bar to show user feedback on script progress
	bar := pb.Simple.Start(len(resources))

	// Wait group and concurrency guard to grab node monitoring data with controlled concurrency
	var wg = &sync.WaitGroup{}
	guard := make(chan struct{}, MaxConcurrency) // Guard to limit max concurrency
	wg.Add(len(resources))

	for _, resource := range resources {
		guard <- struct{}{} // Limits max concurrency to maxGoroutines
		go c.fetchMetricsForResource(resource)
		wg.Done()
		bar.Increment()
		<-guard
	}

	// Block on tasks finishing
	wg.Wait()
	bar.Finish() // complete loading bar for user
}

func (c *ResourceMetricFetcher) fetchMetricsForResource(
	resource *resources.ResourceSummary,
) {
	targets := resource.Resource.GetMetricTargets(resource)
	for statType, metrics := range targets.Targets {
		var metricsToGrab []types.MetricDataQuery
		for _, metric := range metrics {
			metricsToGrab = append(metricsToGrab, types.MetricDataQuery{
				MetricStat: &types.MetricStat{
					Metric: &types.Metric{
						MetricName: aws.String(metric),
						Namespace:  aws.String(targets.Namespace),
						Dimensions: targets.Dimensions,
					},
					Period: aws.Int32(60 * 60 * 24), // 1 day interval
					Stat:   aws.String(statType),
				},
				Id: aws.String(strings.ToLower(metric)),
			})
		}
		startTime := aws.Time(time.Now().Add(time.Duration(-30) * 24 * time.Hour)) // 30 Days ago
		endTime := aws.Time(time.Now())

		data, err := c.CW.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
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
				resource.Metrics = append(resource.Metrics, resources.MetricBlob{
					Name:   *metric.Id,
					Values: metric.Values,
				})
			}
			if data.NextToken != nil {
				data, err = c.CW.GetMetricData(context.TODO(), &cloudwatch.GetMetricDataInput{
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
}
