package cwmessagebatch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
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
