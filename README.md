# ec-usage-analyzer

The ec-usage-analyzer script will fetch information about your Elasticache clusters needed to analyze how efficiently it
is being used.

![Usage Diagram](./docs/imgs/ec-usage-analyzer.png)

## Permissions Needed

This script is currently intended to be run from an operator laptop with READ only permissions.
Specifically you will need following permissions from cloud watch and elasticache.

```
elasticache:DescribeCacheClusters
cloudwatch:GetMetricData
```

## Requirements.

- [Go](https://go.dev/dl/)

## Building:

```console
make
```

## Running

After building the binary for script you can run it like:

```console
chmod +x ./dist/ec-usage-analyzer
AWS_PROFILE=my-profile AWS_REGION=us-west-2 ./dist/ec-usage-analyzer
```

Running the script will produce an output a file named `results.csv` With raw data about your workloads needed for
analysis.
