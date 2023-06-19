package main

import (
	"context"
	"fmt"
	"os"

	"github.com/momentohq/ec-usage-analyzer/internal/handlers"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

func main() {

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithSharedConfigProfile(os.Getenv("AWS_PROFILE")),
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
