package main

import (
	"context"
	"fmt"
	"os"

	"github.com/momentohq/ec-usage-analyzer/internal/handlers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

func main() {

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithSharedConfigProfile(os.Getenv("AWS_PROFILE")),
		config.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxAttempts(retry.NewAdaptiveMode(func(options *retry.AdaptiveModeOptions) {
				options.StandardOptions = []func(*retry.StandardOptions){
					func(options *retry.StandardOptions) {
						// Disable Rate Limiter since we have high number of retries and backoff for this.
						// We dont want to exhaust client side bucket
						options.RateLimiter = NoOpRateLimit{}
					},
				}
			}), 20)
		}),
	)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}

	h := &handlers.Handler{
		Cw: cloudwatch.NewFromConfig(cfg),
		Ec: elasticache.NewFromConfig(cfg),
	}
	err = h.Handle()
	if err != nil {
		panic(err)
	}
}

// NoOpRateLimit Since we are doing a large number of retries for this application we don't want to do any client
// side throttling since we exhaust token bucket easily.
// See this issue comment:
//
//	https://github.com/aws/aws-sdk-go-v2/issues/543#issuecomment-620124268
type NoOpRateLimit struct{}

func (NoOpRateLimit) AddTokens(uint) error { return nil }
func (NoOpRateLimit) GetToken(context.Context, uint) (func() error, error) {
	return noOpToken, nil
}
func noOpToken() error { return nil }
