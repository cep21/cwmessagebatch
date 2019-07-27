package cwmessagebatch

import (
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go/aws/request"
)

func reqWithBody(body string) *request.Request {
	req, err := http.NewRequest("get", "http://www.example.com", nil)
	if err != nil {
		panic(err)
	}
	ret := &request.Request{
		HTTPRequest: req,
	}
	ret.SetStringBody(body)
	return ret
}

func Test_buildPostGZip(t *testing.T) {
	tests := []struct {
		name     string
		arg      *request.Request
		validate func(r *request.Request)
	}{
		{
			name: "basic",
			arg:  reqWithBody("hello world"),
			validate: func(r *request.Request) {
				require.Equal(t, "gzip", r.HTTPRequest.Header.Get("Content-Encoding"))
				require.NoError(t, r.Error)
			},
		},
		{
			name: "should_compress",
			arg:  reqWithBody(strings.Repeat("A", 1024*64)),
			validate: func(r *request.Request) {
				require.NoError(t, r.Error)
			},
		},
		{
			name: "shoulderr",
			arg:  reqWithBody(randomString(1024 * 64)),
			validate: func(r *request.Request) {
				require.Error(t, r.Error)
				require.IsType(t, &awsRequestSizeError{}, r.Error)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			buildPostGZip(tt.arg)
			tt.validate(tt.arg)
		})
	}
}

func randomString(n int) string {
	ret := strings.Builder{}
	for i := 0; i < n; i++ {
		ret.WriteRune(rune(10 + rand.Intn(240)))
	}
	return ret.String()
}

func TestGzipBody(t *testing.T) {
	r := reqWithBody("hi")
	gzipBody(r)
	require.Equal(t, 1, r.Handlers.Build.Len())
}
