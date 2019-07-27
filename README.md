# cwmessagebatch

[![Build Status](https://travis-ci.org/cep21/cwmessagebatch.svg?branch=master)](https://travis-ci.org/cep21/cwmessagebatch)
[![GoDoc](https://godoc.org/github.com/cep21/cwmessagebatch?status.svg)](https://godoc.org/github.com/cep21/cwmessagebatch)
[![Coverage Status](https://coveralls.io/repos/github/cep21/cwmessagebatch/badge.svg)](https://coveralls.io/github/cep21/cwmessagebatch)

cwmessagebatch allows you to send metrics to cloudwatch without worrying
about how many metrics you can reference in a single request or how
to compress or split the metrics when they are too big.

It exposes an interface that matches the cloudwatch
[PutMetricData](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html) interface from
[aws-sdk-go](https://github.com/aws/aws-sdk-go/blob/0bdd50bfa501fa6d8d6db0c2bf2c634fc534d9a1/service/cloudwatch/cloudwatchiface/interface.go#L154).
The idea is that you can call PutMetricData, but can pass as large or as many
datum as you want.

# Rules checked

* Splits MetricDatum into buckets
* Splits large Values arrays from single MetricDatum
* Splits large HTTP request bodies
* gzip encodes request bodies
* Optional filtering or uniformity around UTC timestamps and valid cloudwatch units

# Example

```go
func ExampleAggregator_PutMetricData() {
	a := cwmessagebatch.Aggregator {
		Client: cloudwatch.New(session.Must(session.NewSession(&aws.Config{
			Region: aws.String("us-west-2"),
		}))),
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("custom"),
		MetricData: []*cloudwatch.MetricDatum {
			{
				MetricName: aws.String("custom metric"),
			},
		},
	})
	if err != nil {
		// You'll need valid AWS credentials
		fmt.Println("error result")
	} else {
		fmt.Println("result")
	}
	// Output: error result
}
```

# Contributing

Make sure your tests pass CI/CD pipeline which includes running `make fix lint test` locally.
You'll need an AWS account to verify integration tests, which should also pass `make integration_test`.
I recommend opening a github issue to discuss your ideas before contributing code.