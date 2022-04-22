package s3

import (
    "bytes"
    "log"
    "net/http"
    "os"
       
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

type S3Helper struct {
	s3 *s3.S3
	bucket string
	tenant string
}

func (h S3Helper) UploadFileToS3(key string, fileName string) error {
    
    // open the file for use
    file, err := os.Open(fileName)
    if err != nil {
        return err
    }
    defer file.Close()

    // get the file size and read
    // the file content into a buffer
    fileInfo, _ := file.Stat()
    var size = fileInfo.Size()
    buffer := make([]byte, size)
    file.Read(buffer)

    // config settings: this is where you choose the bucket,
    // filename, content-type and storage class of the file
    // you're uploading
    _, s3err := h.s3.PutObject(&s3.PutObjectInput{
        Bucket:               aws.String(h.bucket),
        Key:                  aws.String(h.tenant + "/" + key),
        ACL:                  aws.String("bucket-owner-full-control"),
        Body:                 bytes.NewReader(buffer),
        ContentLength:        aws.Int64(size),
        ContentType:          aws.String(http.DetectContentType(buffer)),
        ContentDisposition:   aws.String("attachment"),
    })

    return s3err
}

func NewS3Helper(region string, bucket string, tenant string) *S3Helper {
	s, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
        Credentials: credentials.NewSharedCredentials("", "default"),
    })
	
	if err != nil {
        log.Fatalf("Error occured while creating session: %v\n", err)
    }

	return &S3Helper {
		s3: s3.New(s),
		bucket: bucket,
		tenant: tenant,
	}
}
