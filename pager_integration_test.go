// +build integration

package cwpagedmetricput

import "testing"

func TestIntegrationPager(t *testing.T) {
	testPager(t, true)
}
