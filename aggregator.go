package cwmessagebatch

import (
	"context"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type Config struct {
	// True will reset all datum to the UTC timezone before submitting them
	ResetUTC bool
	// True will empty out the "unit" field of datum that have a unit not explicitly documented at
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
	ClearInvalidUnits bool
	// True will *not* use goroutines to send all the batches at once and will send the batches serially after they are
	// created
	SerialSends bool
	// Callback executed when weird datum or RPC calls force us to drop some of the datum from a request we've had to
	// split.
	OnDroppedDatum func(datum *cloudwatch.MetricDatum)
}

type CloudwatchClient interface {
	PutMetricDataWithContext(aws.Context, *cloudwatch.PutMetricDataInput, ...request.Option) (*cloudwatch.PutMetricDataOutput, error)
}

var _ CloudwatchClient = &cloudwatch.CloudWatch{}

type Aggregator struct {
	Client *cloudwatch.CloudWatch
	Config Config
}

func (c *Aggregator) onDroppedDatum(datum *cloudwatch.MetricDatum) {
	if c.Config.OnDroppedDatum != nil {
		c.Config.OnDroppedDatum(datum)
	}
}

func (c *Aggregator) onGo(f func(errIdx int, bucket []*cloudwatch.MetricDatum), errIdx int, bucket []*cloudwatch.MetricDatum) {
	if c.Config.SerialSends {
		f(errIdx, bucket)
		return
	}
	go f(errIdx, bucket)
}

// Note: More difficult to support PutMetricDataRequest since it is not one request.Request, but many

func (c *Aggregator) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	return c.PutMetricDataWithContext(context.Background(), input)
}

// Match API of cloudwatch interface
func (c *Aggregator) PutMetricDataWithContext(ctx aws.Context, input *cloudwatch.PutMetricDataInput, reqs ...request.Option) (*cloudwatch.PutMetricDataOutput, error) {
	reqs = append(reqs, GZipBody)
	if input == nil {
		return c.Client.PutMetricDataWithContext(ctx, input)
	}
	if c.Config.ClearInvalidUnits {
		for i := range input.MetricData {
			input.MetricData[i] = clearInvalidUnits(input.MetricData[i])
		}
	}
	if c.Config.ResetUTC {
		for i := range input.MetricData {
			input.MetricData[i] = resetToUTC(input.MetricData[i])
		}
	}
	splitDatum := make([]*cloudwatch.MetricDatum, 0, len(input.MetricData))
	for _, d := range input.MetricData {
		splitDatum = append(splitDatum, splitLargeValueArray(d)...)
	}
	buckets := bucketDatum(splitDatum)
	err := c.sendBuckets(ctx, input.Namespace, buckets, reqs)
	if err != nil {
		return nil, err
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

func (c *Aggregator) sendBuckets(ctx context.Context, namespace *string, buckets [][]*cloudwatch.MetricDatum, reqs []request.Option) error {
	errs := make([]error, len(buckets))
	wg := sync.WaitGroup{}
	for i, bucket := range buckets {
		wg.Add(1)
		c.onGo(func(errIdx int, bucket []*cloudwatch.MetricDatum) {
			defer wg.Done()
			errs[errIdx] = c.sendDatum(ctx, namespace, bucket, reqs)
		}, i, bucket)
	}
	wg.Wait()
	err := consolidateErr(errs)
	if err != nil {
		return err
	}
	return nil
}

func resetToUTC(datum *cloudwatch.MetricDatum) *cloudwatch.MetricDatum {
	if datum == nil || datum.Timestamp == nil {
		return datum
	}
	datum.Timestamp = aws.Time(datum.Timestamp.UTC())
	return datum
}

func clearInvalidUnits(datum *cloudwatch.MetricDatum) *cloudwatch.MetricDatum {
	if datum == nil || datum.Unit == nil {
		return datum
	}
	datum.Unit = filterInvalidUnit(*datum.Unit)
	return datum
}

// Documented on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html under
// "the Values and Counts method enables you to publish up to 150 values per metric with one PutMetricData request"
const maxValuesSize = 150

func splitLargeValueArray(in *cloudwatch.MetricDatum) []*cloudwatch.MetricDatum {
	if in == nil {
		return nil
	}
	if len(in.Values) <= maxValuesSize {
		// No fixing required
		return []*cloudwatch.MetricDatum{in}
	}
	lastDatum := *in
	ret := make([]*cloudwatch.MetricDatum, 0, 1+len(lastDatum.Values)/maxValuesSize)
	for len(lastDatum.Values) > maxValuesSize {
		lastSizeDatum := lastDatum
		// Honestly not sure what to do here .... what is cloudwatch thinking?
		// Need to experiment about the right thing to do here.
		//lastSizeDatum.StatisticValues = nil

		lastSizeDatum.Values = lastDatum.Values[0:maxValuesSize]
		if lastSizeDatum.Counts != nil {
			lastSizeDatum.Counts = lastDatum.Counts[0:maxValuesSize]
		}
		ret = append(ret, &lastSizeDatum)
		lastDatum.Values = lastDatum.Values[maxValuesSize:]
		if lastSizeDatum.Counts != nil {
			lastDatum.Counts = lastDatum.Counts[maxValuesSize:]
		}
	}
	if in.StatisticValues != nil && len(ret) < int(*in.StatisticValues.SampleCount) {
		// Give one value from ret to each datum we're sending's stat set
		for _, d := range ret {
			d.StatisticValues = &cloudwatch.StatisticSet{
				SampleCount: aws.Float64(1),
				Sum:         aws.Float64(0),
				Maximum:     in.StatisticValues.Maximum,
				Minimum:     in.StatisticValues.Minimum,
			}
		}
		lastDatum.StatisticValues.SampleCount = aws.Float64(*lastDatum.StatisticValues.SampleCount - float64(len(ret)))
	}
	ret = append(ret, &lastDatum)
	return ret
}

// Documented on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html under
// "Each request is also limited to no more than 20 different metrics"
const maxDatumSize = 20

func bucketDatum(in []*cloudwatch.MetricDatum) [][]*cloudwatch.MetricDatum {
	ret := make([][]*cloudwatch.MetricDatum, 0, 1+len(in)/maxDatumSize)
	for len(in) > maxDatumSize {
		ret = append(ret, in[0:maxDatumSize])
		in = in[maxDatumSize:]
	}
	ret = append(ret, in)
	return ret
}

func (c *Aggregator) sendDatum(ctx context.Context, namespace *string, datum []*cloudwatch.MetricDatum, reqs []request.Option) error {
	if len(datum) == 0 {
		return nil
	}
	_, err := c.Client.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		MetricData: datum,
		Namespace:  namespace,
	}, reqs...)
	if err == nil {
		return nil
	}
	if _, isRequestSizeErr := err.(requestSizeError); isRequestSizeErr {
		// Split the request
		if len(datum) == 1 {
			c.onDroppedDatum(datum[0])
			// hmmm that's strange
			return err
		}
		mid := len(datum) / 2
		datums := [][]*cloudwatch.MetricDatum{
			datum[0:mid], datum[mid:],
		}
		return c.sendBuckets(ctx, namespace, datums, reqs)
	}
	for _, d := range datum {
		c.onDroppedDatum(d)
	}
	return err
}

var validUnits = make(map[string]struct{})
var validUnitsOnce sync.Once

func filterInvalidUnit(m string) *string {
	validUnitsOnce.Do(func() {
		// A copy/pasta of valid units listed on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
		const copyPasta = "Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes | Gigabytes | Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent | Count | Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second | Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | Gigabits/Second | Terabits/Second | Count/Second | None"
		for _, part := range strings.Split(copyPasta, "|") {
			part = strings.Trim(part, " ")
			validUnits[part] = struct{}{}
		}
	})
	if _, exists := validUnits[m]; !exists {
		return nil
	}
	return &m
}

func filterNil(errs []error) []error {
	if len(errs) == 0 {
		return errs
	}
	ret := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			ret = append(ret, err)
		}
	}
	return ret
}

func consolidateErr(err []error) error {
	err = filterNil(err)
	if len(err) == 0 {
		return nil
	}
	if len(err) == 1 {
		return err[0]
	}
	return &multiErr{err: err}
}

type multiErr struct {
	err []error
}

var _ error = &multiErr{}

func (m *multiErr) Error() string {
	ret := "Multiple errors: "
	for _, e := range m.err {
		ret += e.Error()
	}
	return ret
}
