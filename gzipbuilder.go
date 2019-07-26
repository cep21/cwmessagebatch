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
	putMetricDataKBRequestSizeLimit = 38 * 1000
)

type awsRequestSizeError struct {
	size int
}

func (e *awsRequestSizeError) Error() string {
	return fmt.Sprintf("%s: size=%d", e.Message(), e.size)
}

func (e *awsRequestSizeError) Code() string {
	return "RequestSizeError"
}

func (e *awsRequestSizeError) Message() string {
	return "request size too large"
}

func (e *awsRequestSizeError) OrigErr() error {
	return nil
}

func (e *awsRequestSizeError) RequestSizeError() {
}

type requestSizeError interface {
	RequestSizeError()
}

var _ awserr.Error = &awsRequestSizeError{}
var _ requestSizeError = &awsRequestSizeError{}

// buildPostGZip construct a gzip'd post request.  Put this *after* the regular handler so it can
// use the built in SDK logic to compress the request body
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

func GZipBody(req *request.Request) {
	// Add the GZip handler
	gzipHandler := request.NamedHandler{Name: "cwmessagebatch.gzip", Fn: buildPostGZip}
	// Protect from double adds
	req.Handlers.Build.Remove(gzipHandler)
	req.Handlers.Build.PushBackNamed(gzipHandler)
}
