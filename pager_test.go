package cwpagedmetricput

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/stretchr/testify/require"
)

func Test_clearInvalidUnits(t *testing.T) {
	type args struct {
		datum *cloudwatch.MetricDatum
	}
	tests := []struct {
		name string
		args args
		want *cloudwatch.MetricDatum
	}{
		{
			name: "good_unit",
			args: args{
				datum: &cloudwatch.MetricDatum{
					Unit: aws.String("Seconds"),
				},
			},
			want: &cloudwatch.MetricDatum{
				Unit: aws.String("Seconds"),
			},
		},
		{
			name: "bad_unit",
			args: args{
				datum: &cloudwatch.MetricDatum{
					Unit: aws.String("Second"),
				},
			},
			want: &cloudwatch.MetricDatum{},
		},
		{
			name: "nilunit",
			args: args{
				datum: &cloudwatch.MetricDatum{},
			},
			want: &cloudwatch.MetricDatum{},
		},
		{
			name: "nildatum",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := clearInvalidUnits(tt.args.datum); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clearInvalidUnits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_consolidateErr(t *testing.T) {
	tests := []struct {
		name     string
		args     []error
		validate func(error)
	}{
		{
			name: "nil",
			args: nil,
			validate: func(err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "single",
			args: []error{errors.New("single")},
			validate: func(err error) {
				require.Equal(t, "single", err.Error())
			},
		},
		{
			name: "manynil",
			args: []error{nil, nil},
			validate: func(err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "many",
			args: []error{errors.New("first"), errors.New("second")},
			validate: func(err error) {
				require.Equal(t, "multiple errors: first,second", err.Error())
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.validate(consolidateErr(tt.args))
		})
	}
}

func splitDatumMatch(t *testing.T, in *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum) {
	if in.StatisticValues == nil {
		for _, o := range out {
			require.Nil(t, o.StatisticValues)
		}
	}
	if in.StatisticValues != nil {
		expected := cloudwatch.StatisticSet{
			SampleCount: aws.Float64(0),
			Sum:         aws.Float64(0),
		}
		for _, o := range out {
			require.NotNil(t, o.StatisticValues)
			if expected.Maximum == nil {
				expected.Maximum = o.StatisticValues.Maximum
				expected.Minimum = o.StatisticValues.Minimum
			}
			expected.SampleCount = aws.Float64(*expected.SampleCount + *o.StatisticValues.SampleCount)
			expected.Sum = aws.Float64(*expected.Sum + *o.StatisticValues.Sum)
		}
		require.Equal(t, *in.StatisticValues, expected)
	}
	if in.Value != nil {
		require.Equal(t, 1, len(out))
		require.Equal(t, in, out[0])
	}
	valueCounts := make(map[float64]int)
	for _, o := range out {
		for i := range o.Values {
			c := 1
			if o.Counts != nil {
				c = int(*o.Counts[i])
			}
			valueCounts[*o.Values[i]] += c
		}
	}
	if in.Values != nil {
		for i := range in.Values {
			c := 1
			if in.Counts != nil {
				c = int(*in.Counts[i])
			}
			require.Equal(t, c, valueCounts[*in.Values[i]])
		}
	}
}

func Test_splitLargeValueArray(t *testing.T) {
	var fewVals cloudwatch.MetricDatum
	makeDatum(&fewVals, randoms(100, 1024*1024, 2))
	var manyVals cloudwatch.MetricDatum
	makeDatum(&manyVals, randoms(1024, 1024*1024, 2))
	tests := []struct {
		name   string
		args   *cloudwatch.MetricDatum
		verify func(in *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum)
	}{
		{
			name: "nil",
			args: nil,
			verify: func(_ *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum) {
				require.Nil(t, out)
			},
		},
		{
			name: "smallsize",
			args: &cloudwatch.MetricDatum{},
			verify: func(in *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum) {
				require.Equal(t, 1, len(out))
				require.Equal(t, in, out[0])
			},
		},
		{
			name: "fewvals",
			args: &fewVals,
			verify: func(in *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum) {
				splitDatumMatch(t, in, out)
			},
		},
		{
			name: "manyvals",
			args: &manyVals,
			verify: func(in *cloudwatch.MetricDatum, out []*cloudwatch.MetricDatum) {
				splitDatumMatch(t, in, out)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			out := splitLargeValueArray(tt.args)
			tt.verify(tt.args, out)
		})
	}
}
