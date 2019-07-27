package cwmessagebatch

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awsutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type fetchableClient interface {
	PutMetricDataWithContext(ctx aws.Context, in *cloudwatch.PutMetricDataInput, opts ...request.Option) (*cloudwatch.PutMetricDataOutput, error)
	GetMetricStatistics(input *cloudwatch.GetMetricStatisticsInput) (*cloudwatch.GetMetricStatisticsOutput, error)
}

func key(name *string, dims []*cloudwatch.Dimension) string {
	ret := *name
	d2 := make([]*cloudwatch.Dimension, 0, len(dims))
	d2 = append(d2, dims...)
	sort.Slice(dims, func(i, j int) bool {
		return *dims[i].Name < *dims[j].Name
	})
	for _, d := range dims {
		ret += *d.Name
		ret += ":"
		ret += *d.Value
		ret += ":"
	}
	return ret
}

type memoryCloudWatchClient struct {
	errOnCall int
	err       error
	in        []*cloudwatch.PutMetricDataInput
	mu        sync.Mutex

	aggregation map[string]*cloudwatch.StatisticSet
	vals        map[string]*cloudwatch.MetricDatum
}

func (m *memoryCloudWatchClient) GetMetricStatistics(input *cloudwatch.GetMetricStatisticsInput) (*cloudwatch.GetMetricStatisticsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(input.ExtendedStatistics) != 0 {
		panic("unimplemented")
	}
	dk := key(input.MetricName, input.Dimensions)

	agg := m.aggregation[dk]
	if agg == nil {
		fmt.Println(awsutil.Prettify(m.aggregation))
		return &cloudwatch.GetMetricStatisticsOutput{}, nil
	}

	return &cloudwatch.GetMetricStatisticsOutput{
		Datapoints: []*cloudwatch.Datapoint{
			{
				Maximum:     agg.Maximum,
				Minimum:     agg.Minimum,
				Sum:         agg.Sum,
				SampleCount: agg.SampleCount,
				Timestamp:   input.StartTime,
			},
		},
	}, nil
}

func (m *memoryCloudWatchClient) PutMetricDataWithContext(ctx aws.Context, in *cloudwatch.PutMetricDataInput, opts ...request.Option) (*cloudwatch.PutMetricDataOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.in = append(m.in, in)
	if len(m.in) == m.errOnCall {
		return nil, m.err
	}
	if m.aggregation == nil {
		m.aggregation = make(map[string]*cloudwatch.StatisticSet)
		m.vals = make(map[string]*cloudwatch.MetricDatum)
	}
	for _, d := range in.MetricData {
		if d.StatisticValues == nil && d.Values == nil && d.Value == nil {
			return nil, errors.New("expect something")
		}
		dk := key(d.MetricName, d.Dimensions)
		if d.StatisticValues != nil {
			if m.aggregation[dk] == nil {
				m.aggregation[dk] = &cloudwatch.StatisticSet{
					SampleCount: aws.Float64(0),
					Sum:         aws.Float64(0),
					Minimum:     d.StatisticValues.Minimum,
					Maximum:     d.StatisticValues.Maximum,
				}
			}
			m.aggregation[dk].SampleCount = aws.Float64(*m.aggregation[dk].SampleCount + *d.StatisticValues.SampleCount)
			m.aggregation[dk].Sum = aws.Float64(*m.aggregation[dk].Sum + *d.StatisticValues.Sum)
			m.aggregation[dk].Minimum = aws.Float64(math.Min(*m.aggregation[dk].Minimum, *d.StatisticValues.Minimum))
			m.aggregation[dk].Maximum = aws.Float64(math.Max(*m.aggregation[dk].Maximum, *d.StatisticValues.Maximum))
		}
		if m.vals[dk] == nil {
			m.vals[dk] = &cloudwatch.MetricDatum{}
		}
		var allVals []*float64
		var allCounts []*float64
		if d.Value != nil {
			allVals = append(allVals, d.Value)
			allCounts = append(allCounts, aws.Float64(1.0))
		}
		for i := range d.Values {
			allVals = append(allVals, d.Values[i])
			if len(d.Counts) == 0 {
				allCounts = append(allCounts, aws.Float64(1.0))
			} else {
				allCounts = append(allCounts, d.Counts[i])
			}
		}
		if len(allVals) != 0 {
			m.vals[dk].Values = append(m.vals[dk].Values, allVals...)
			m.vals[dk].Counts = append(m.vals[dk].Counts, allVals...)
			if d.StatisticValues == nil {
				if m.aggregation[dk] == nil {
					m.aggregation[dk] = &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(0),
						Sum:         aws.Float64(0),
						Minimum:     allVals[0],
						Maximum:     allVals[0],
					}
				}
				for i := range allVals {
					m.aggregation[dk].SampleCount = aws.Float64(*m.aggregation[dk].SampleCount + *allCounts[i])
					m.aggregation[dk].Sum = aws.Float64(*m.aggregation[dk].Sum + *allCounts[i]**allVals[i])
					m.aggregation[dk].Minimum = aws.Float64(math.Min(*m.aggregation[dk].Minimum, *allVals[i]))
					m.aggregation[dk].Maximum = aws.Float64(math.Max(*m.aggregation[dk].Maximum, *allVals[i]))
				}
			}
		}
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

var _ CloudWatchClient = &memoryCloudWatchClient{}
var _ fetchableClient = &memoryCloudWatchClient{}
var _ fetchableClient = &cloudwatch.CloudWatch{}

func TestAggregator(t *testing.T) {
	testAggregator(t, false)
}
