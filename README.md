# ec-usage-analyzer

Script used to read Elasticache clusters and fetch data needed to analyze how efficiently it is
being used.

## Requirements.

- [Go](https://go.dev/dl/)

## Building:

```console
make
```

## Running
Can run the built binary like
```console
AWS_PROFILE=my-profile AWS_REGION=us-west-2 ./dist/ec-usage-analyzer
```

Will output a file named `results.csv` With raw data about your workloads needed for analysis.
