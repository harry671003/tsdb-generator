package s3

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxPartSize   = int64(100 * 1024 * 1024)
	maxRetries    = 1
	multipartSize = int64(100 * 1024 * 1024)
)

type S3Helper struct {
	s3     *s3.S3
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

	bucketKey := h.tenant + "/" + key

	if size > multipartSize {
		return h.multipartUpload(bucketKey, &buffer, size)
	}

	return h.upload(bucketKey, &buffer, size)
}

func (h S3Helper) upload(objectKey string, buffer *[]byte, size int64) error {
	_, s3err := h.s3.PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(h.bucket),
		Key:                aws.String(objectKey),
		ACL:                aws.String("bucket-owner-full-control"),
		Body:               bytes.NewReader(*buffer),
		ContentLength:      aws.Int64(size),
		ContentType:        aws.String(http.DetectContentType(*buffer)),
		ContentDisposition: aws.String("attachment"),
	})
	return s3err
}

func (h S3Helper) multipartUpload(objectKey string, buffer *[]byte, size int64) error {
	log.Printf("Uploading %s using multipart\n", objectKey)

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(h.bucket),
		Key:         aws.String(objectKey),
		ContentType: aws.String(http.DetectContentType(*buffer)),
	}

	resp, err := h.s3.CreateMultipartUpload(input)
	if err != nil {
		return err
	}

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		partErr error
	)

	for curr = 0; remaining != 0; curr += partLength {
		wg.Add(1)

		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}

		go func(curr int64, partNumber int, remaining int64, partLength int64) {
			defer wg.Done()

			completedPart, err := h.uploadPart(resp, (*buffer)[curr:curr+partLength], partNumber)
			if err != nil {
				fmt.Printf("Error :%v", err)
				err := h.abortMultipartUpload(resp)
				if err != nil {
					fmt.Printf("Error :%v", err)
					partErr = err
					return
				}
				partErr = err
				return
			}

			mu.Lock()
			defer mu.Unlock()
			completedParts = append(completedParts, completedPart)
		}(curr, partNumber, remaining, partLength)

		remaining -= partLength
		partNumber++
	}

	wg.Wait()
	if partErr != nil {
		return partErr
	}

	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	_, err = h.completeMultipartUpload(resp, completedParts)
	if err != nil {
		fmt.Printf("Error :%v", err)
		return err
	}

	return nil
}

func (h S3Helper) completeMultipartUpload(resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return h.s3.CompleteMultipartUpload(completeInput)
}

func (h S3Helper) uploadPart(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= 3 {
		uploadResult, err := h.s3.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			tryNum++
		} else {
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func (h S3Helper) abortMultipartUpload(resp *s3.CreateMultipartUploadOutput) error {
	log.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := h.s3.AbortMultipartUpload(abortInput)
	return err
}

func NewS3Helper(region string, bucket string, tenant string) *S3Helper {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewSharedCredentials("", "default"),
	})

	if err != nil {
		log.Fatalf("Error occured while creating session: %v\n", err)
	}

	return &S3Helper{
		s3:     s3.New(s),
		bucket: bucket,
		tenant: tenant,
	}
}
