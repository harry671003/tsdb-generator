package main

import (
	"flag"
	"log"
	"strings"

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
		log.Printf("Getting files for block: %v\n", block)
		files := util.GetFilesInBlock("data/" + block)
		log.Printf("Found files %v\n", *files)

		for _, file := range *files {
			s3Key := strings.ReplaceAll(file, "data/", "")
			log.Printf("Uploading file: %s\n", s3Key)
			if err := s3Helper.UploadFileToS3(s3Key, file); err != nil {
				log.Fatalf("Error while uploading file: %v", err)
			}
		}
	}
}
