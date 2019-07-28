/*
Package cwpagedmetricput allows mass PutMetricData calls into CloudWatch metrics
with an API that matches CloudWatch's aws-sdk-go client.  It does this by bucketing
Datum into sizes allowed by AWS's API.  It also takes cost saving measures by gzip sending large
datum sends.
*/
package cwpagedmetricput
