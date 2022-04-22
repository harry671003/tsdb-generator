package main

import (
	"flag"
	"log"
	"strings"
	"sync"

	"github.com/harry671003/tsdb-generator/pkg/s3"
	"github.com/harry671003/tsdb-generator/pkg/util"
)

var (
	dataDir = flag.String("d", "./data",
		"The data directory.")
	tenantId = flag.String("t", "027400258944_ws-cb6db68f-35bc-4ad7-9cd5-254c222457d2",
		"Thanos/Cortex tenantID")
	s3Bucket = flag.String("b", "cortex-block-storage-309956603123",
		"Output directory to generate TSDB blocks in")
	awsRegion = flag.String("r", "us-west-2",
		"Output directory to generate TSDB blocks in")
)

func main() {
	log.Println("Uploading blocks to S3")
	flag.Parse()

	blocks := util.GetBlocks(*dataDir)
	log.Printf("Found blocks: %v", *blocks)

	s3Helper := s3.NewS3Helper(*awsRegion, *s3Bucket, *tenantId)

	for _, block := range *blocks {
		uploadBlock(block, s3Helper)
	}
}

func uploadBlock(block string, s3Helper *s3.S3Helper) {
	files := util.GetFilesInBlock("data/" + block)
	log.Printf("Uploading files for block %s: %v\n", block, *files)

	var wg sync.WaitGroup
	for _, file := range *files {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			s3Key := strings.ReplaceAll(file, "data/", "")
			if err := s3Helper.UploadFileToS3(s3Key, file); err != nil {
				log.Fatalf("Error while uploading file: %v", err)
			}
			log.Printf("Uploaded file: %s\n", s3Key)
		}(file)
	}
	wg.Wait()
}
