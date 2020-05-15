package benchclient

import (
	"bytes"
	"io"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
)

var (
	// The session the S3 Downloader will use
	AWSSession = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("us-east-1")},
	}))
)

type S3 struct {
	bucket string
	log    logger.ILogger
}

func NewS3(bk string) *S3 {
	return &S3{
		bucket: bk,
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  "S3: ",
		},
	}
}

func (c *S3) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	reqId := uuid.New().String()

	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, true
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(AWSSession)

	// Upload the file to S3.
	start := time.Now()
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(val),
	})
	if err != nil {
		c.log.Error("failed to upload file: %v", err)
		return reqId, false
	}
	c.log.Info("Set %s %d", key, int64(time.Since(start)))

	return reqId, true
}

func (c *S3) EcGet(key string, size int, args ...interface{}) (string, io.ReadCloser, bool) {
	reqId := uuid.New().String()

	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, true
	}

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(AWSSession)

	// Write the contents of S3 Object to the file
	buff := &aws.WriteAtBuffer{}
	start := time.Now()
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		c.log.Error("failed to download file: %v", err)
		return reqId, nil, false
	}
	c.log.Info("Get %s %d", key, int64(time.Since(start)))

	data := buff.Bytes()
	return reqId, ioutil.NopCloser(bytes.NewReader(data)), true
}

func (c *S3) Close() {
	// Nothing
}
