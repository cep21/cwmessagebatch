package cwpagedmetricput_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/cep21/cwpagedmetricput"
)

func ExamplePager_PutMetricData() {
	a := cwpagedmetricput.Pager{
		Client: cloudwatch.New(session.Must(session.NewSession())),
	}
	_, _ = a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("custom"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String("custom metric"),
				Value:      aws.Float64(1.0),
			},
		},
	})
	// Output:
}
