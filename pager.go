package cwpagedmetricput

import (
	"context"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

// Config controls optional parameters of Pager. The zero value is a reasonable default.
type Config struct {
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

// CloudWatchClient is anything that can receive CloudWatch metrics as documented by CloudWatch's public API constraints.
type CloudWatchClient interface {
	// PutMetricDataWithContext should match the contract of cloudwatch.CloudWatch.PutMetricDataWithContext
	PutMetricDataWithContext(aws.Context, *cloudwatch.PutMetricDataInput, ...request.Option) (*cloudwatch.PutMetricDataOutput, error)
}

// The API of aggregator matches the API of cloudwatch
var _ CloudWatchClient = &cloudwatch.CloudWatch{}
var _ CloudWatchClient = &Pager{}

// Pager behaves like CloudWatch's MetricData API, but takes care of all of the smaller parts for you around
// how to correctly bucket and split MetricDatum.
// Pager is as thread safe as the Client parameter.  If you're using *cloudwatch.CloudWatch as your
// Client, then it will be thread safe.
type Pager struct {
	// Client is required and is usually an instance of *cloudwatch.CloudWatch
	Client CloudWatchClient
	// Config is optional and controls how data is filtered or aggregated
	Config Config
}

// onDroppedDatum optionally calls the Config's OnDroppedDatum if the API splits a request and is unable
// to send all the datum.
func (c *Pager) onDroppedDatum(datum *cloudwatch.MetricDatum) {
	if c.Config.OnDroppedDatum != nil {
		c.Config.OnDroppedDatum(datum)
	}
}

// onGo is a `go` alternative that we call to abstract out if a function should execute serially or in concurrently.
func (c *Pager) onGo(f func(errIdx int, bucket []*cloudwatch.MetricDatum), errIdx int, bucket []*cloudwatch.MetricDatum) {
	if c.Config.SerialSends {
		f(errIdx, bucket)
		return
	}
	go f(errIdx, bucket)
}

// PutMetricData should be a drop in replacement for *cloudwatch.CloudWatch.PutMetricData, but
// taking care of splitting datum that are too large.
// Note: More difficult to support PutMetricDataRequest since it is not one request.Request, but many.
func (c *Pager) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	return c.PutMetricDataWithContext(context.Background(), input)
}

// PutMetricDataWithContext should be a drop in replacement for *cloudwatch.CloudWatch.PutMetricDataWithContext, but
// taking care of splitting datum that are too large.
func (c *Pager) PutMetricDataWithContext(ctx aws.Context, input *cloudwatch.PutMetricDataInput, reqs ...request.Option) (*cloudwatch.PutMetricDataOutput, error) {
	if input == nil {
		// Fallback behaviour is whatever the client does for nil input
		return c.Client.PutMetricDataWithContext(ctx, input)
	}
	// Appending gzip is optional but useful to reduce the total size of the request
	// Also save you money since you are billed per request.
	reqs = append(reqs, gzipBody)
	// Process optional rules first
	if c.Config.ClearInvalidUnits {
		for i := range input.MetricData {
			input.MetricData[i] = clearInvalidUnits(input.MetricData[i])
		}
	}

	// Split each individual datum that has too many .Values items into multiple datum
	splitDatum := make([]*cloudwatch.MetricDatum, 0, len(input.MetricData))
	for _, d := range input.MetricData {
		splitDatum = append(splitDatum, splitLargeValueArray(d)...)
	}

	// Split too many datum inside this call into multiple calls
	buckets := bucketDatum(splitDatum)

	// Send all the datum at once
	err := c.sendBuckets(ctx, input.Namespace, buckets, reqs)
	if err != nil {
		return nil, err
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

// sendBuckets executes sendDatum on all the buckets in parallel.  It returns when all buckets finish executing.
func (c *Pager) sendBuckets(ctx context.Context, namespace *string, buckets [][]*cloudwatch.MetricDatum, reqs []request.Option) error {
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
	return consolidateErr(errs)
}

// clearInvalidUnits returns datum with Unit fields filtered of invalid values
func clearInvalidUnits(datum *cloudwatch.MetricDatum) *cloudwatch.MetricDatum {
	if datum == nil || datum.Unit == nil {
		return datum
	}
	datum.Unit = filterInvalidUnit(datum.Unit)
	return datum
}

// Documented on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html under
// "the Values and Counts method enables you to publish up to 150 values per metric with one PutMetricData request"
const maxValuesSize = 150

// splitLargeValueArray splits a single datum if the size of the values array is larger than CloudWatch's
// API allows.  It also takes care of correcting the StatisticValues set for the split datum.
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
		// Notice how each lastSizeDatum does not have a StatisticValues set.
		// See below for loop.
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
		// Honestly not sure what to do here .... what is cloudwatch thinking?
		// It isn't well documented on the site, but the right behaviour here according to
		// various integration tests is to keep the larger StatisticValues on
		// lastDatum while we "fake" a StatisticSet on each of the other datum that contains
		// at least one item
		for _, d := range ret {
			d.StatisticValues = &cloudwatch.StatisticSet{
				SampleCount: aws.Float64(1),
				Sum:         aws.Float64(0),
				Maximum:     in.StatisticValues.Maximum,
				Minimum:     in.StatisticValues.Minimum,
			}
		}
		tmp := *in.StatisticValues
		lastDatum.StatisticValues = &tmp
		lastDatum.StatisticValues.SampleCount = aws.Float64(*lastDatum.StatisticValues.SampleCount - float64(len(ret)))
	}
	ret = append(ret, &lastDatum)
	return ret
}

// Documented on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html under
// "Each request is also limited to no more than 20 different metrics"
const maxDatumSize = 20

// bucketDatum splits a single bulk request to send datum into multiple bulk requests, limiting each send
// to CloudWatch's limited size.
func bucketDatum(in []*cloudwatch.MetricDatum) [][]*cloudwatch.MetricDatum {
	ret := make([][]*cloudwatch.MetricDatum, 0, 1+len(in)/maxDatumSize)
	for len(in) > maxDatumSize {
		ret = append(ret, in[0:maxDatumSize])
		in = in[maxDatumSize:]
	}
	ret = append(ret, in)
	return ret
}

// sendDatum will construct PutMetricDataInput objects and send them to c.Client.  If any of these sends fail because
// the sent request body would be too big, the datum array is split into halves and sent separately.
func (c *Pager) sendDatum(ctx context.Context, namespace *string, datum []*cloudwatch.MetricDatum, reqs []request.Option) error {
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
			// Even a single datum is too large.  This is very strange.  The best we can do is drop this
			// single datum.  It will never work.
			c.onDroppedDatum(datum[0])
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

// These two variables are used by filterInvalidUnit to cache proessing of valid units
var validUnits = make(map[string]struct{})
var validUnitsOnce sync.Once

// filterInvalidUnit returns nil if m is an invalid unit, otherwise it returns m
func filterInvalidUnit(m *string) *string {
	if m == nil {
		return nil
	}
	validUnitsOnce.Do(func() {
		// A copy/pasta of valid units listed on https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
		const copyPasta = "Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes | Gigabytes | Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent | Count | Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second | Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | Gigabits/Second | Terabits/Second | Count/Second | None"
		for _, part := range strings.Split(copyPasta, "|") {
			part = strings.Trim(part, " ")
			validUnits[part] = struct{}{}
		}
	})
	if _, exists := validUnits[*m]; !exists {
		return nil
	}
	return m
}

// filterNil removes nil errors from an array
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

// consolidateErr turns multiple errors into a single error
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

// multiErr is an error that is actually multiple errors at once.
type multiErr struct {
	err []error
}

var _ error = &multiErr{}

// Error returns a combined error string
func (m *multiErr) Error() string {
	ret := "multiple errors: "
	for i, e := range m.err {
		if i != 0 {
			ret += ","
		}
		ret += e.Error()
	}
	return ret
}
