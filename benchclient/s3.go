package benchclient

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	// The session the S3 Downloader will use
	AWSSession = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("us-east-1")},
	}))
)

type S3 struct {
	*defaultClient
	bucket string
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

func NewS3(bk string) *S3 {
	client := &S3{
		defaultClient: newDefaultClient("S3: "),
		bucket: bk,
		uploader: s3manager.NewUploader(AWSSession),
		downloader: s3manager.NewDownloader(AWSSession),
	}
	client.setter = client.set
	client.getter = client.get
	return client
}

func (c *S3) set(key string, val []byte) error {
	// Upload the file to S3.
	_, err := c.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(val),
	})
	return err
}

func (c *S3) get(key string, size int) (io.ReadCloser, error) {
	var buff *aws.WriteAtBuffer
	if size > 0 {
		buff = aws.NewWriteAtBuffer(make([]byte, 0, size))
	} else {
		buff = new(aws.WriteAtBuffer)
	}
	_, err := c.downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	} else {
		return ioutil.NopCloser(bytes.NewReader(buff.Bytes())), nil
	}
}
