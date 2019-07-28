package cwpagedmetricput

import (
	"errors"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
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
		name    string
		args    []error
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
