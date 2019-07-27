// +build integration

package cwmessagebatch

import "testing"

func TestIntegrationAggregator(t *testing.T) {
	testAggregator(t, true)
}
