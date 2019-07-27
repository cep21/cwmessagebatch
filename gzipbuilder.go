package cwmessagebatch

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
)

const (
	// On https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html they document
	// a limit of "Each PutMetricData request is limited to 40 KB in size for HTTP POST requests"
	// Of course they include the headers in their value.  So, we take the 40k and subtract some extra meta information
	// to give us a comfortable limit and that's how we get 38k
	putMetricDataKBRequestSizeLimit = 38 * 1024
)

// awsRequestSizeError is an internal only type that we use to signal up the call stack that a
// request will be too large for AWS's API.
type awsRequestSizeError struct {
	size int
}

// Error satisfies the interface of `error` and returns an error message about the
// expected size.
func (e *awsRequestSizeError) Error() string {
	return fmt.Sprintf("request size too large: size=%d", e.size)
}

// RequestSizeError is a marker interface to signal a returned error is due to a large body size
func (e *awsRequestSizeError) RequestSizeError() {
}

// requestSizeError is the private type set on request errors that result from building a gzip'd body
// that is too large
type requestSizeError interface {
	RequestSizeError()
	error
}

var _ requestSizeError = &awsRequestSizeError{}

// buildPostGZip construct a gzip'd post request.  Put this *after* the regular handler so it can
// use the built in SDK logic to compress the request body.  Will set an error with method `RequestSizeError`
// on the request if the compressed body is too large for API_PutMetricData's API
func buildPostGZip(r *request.Request) {
	r.HTTPRequest.Header.Set("Content-Encoding", "gzip")

	// Construct a byte buffer and gzip writer
	var w bytes.Buffer
	gzipW := gzip.NewWriter(&w)

	// GZip the body
	_, err := io.Copy(gzipW, r.GetBody())
	if err != nil {
		r.Error = awserr.New(request.ErrCodeSerialization, "failed encoding gzip", err)
		return
	}
	err = gzipW.Close()
	if err != nil {
		r.Error = awserr.New(request.ErrCodeSerialization, "failed closing gzip writer", err)
		return
	}

	// Check the size of the request to determine whether the client should further split the request
	if len(w.Bytes()) > putMetricDataKBRequestSizeLimit {
		r.Error = &awsRequestSizeError{
			size: len(w.Bytes()),
		}
		return
	}
	r.SetBufferBody(w.Bytes())
}

var gzipHandler = request.NamedHandler{Name: "cwmessagebatch.gzip", Fn: buildPostGZip}

// gzipBody attaches a gzip handler to the Build phase of the eventual AWS request
func gzipBody(req *request.Request) {
	// Protect from double adds
	req.Handlers.Build.Remove(gzipHandler)
	req.Handlers.Build.PushBackNamed(gzipHandler)
}
