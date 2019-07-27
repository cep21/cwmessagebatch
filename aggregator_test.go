package cwmessagebatch

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/stretchr/testify/require"
)

func Test_resetToUTC(t *testing.T) {
	type args struct {
		datum *cloudwatch.MetricDatum
	}
	tests := []struct {
		name string
		args args
		// Want returns an error if datum fails
		verify func(t *testing.T, d *cloudwatch.MetricDatum)
	}{
		{
			name: "nil",
			args: args{
				datum: nil,
			},
			verify: func(t *testing.T, d *cloudwatch.MetricDatum) {
				require.Nil(t, d)
			},
		},
		{
			name: "nil_ts",
			args: args{
				datum: &cloudwatch.MetricDatum{},
			},
			verify: func(t *testing.T, d *cloudwatch.MetricDatum) {
				require.Nil(t, d.Timestamp)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := resetToUTC(tt.args.datum)
			tt.verify(t, got)
		})
	}
}

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
