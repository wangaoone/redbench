package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"io"
	"io/ioutil"
	"time"
)

type S3Client struct {
	bucket string
	log    logger.ILogger
}

func NewS3Client(bk string) *S3Client {
	return &S3Client{
		bucket: bk,
		log:    &logger.ColorLogger{
			Verbose: true,
			Level: logger.LOG_LEVEL_ALL,
			Color: true,
			Prefix: "S3Client ",
		},
	}
}

func (c *S3Client) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	reqId := uuid.New().String()

	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, true
	}

	// The session the S3 Uploader will use
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("us-east-1")},
	}))

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

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

func (c *S3Client) EcGet(key string, size int, args ...interface{}) (string, io.ReadCloser, bool) {
	reqId := uuid.New().String()

	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, true
	}

	// The session the S3 Downloader will use
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("us-east-1")},
	}))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

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
