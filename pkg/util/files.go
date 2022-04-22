package util

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/oklog/ulid"
)

func GetBlocks(path string) *[]string {
	blocks := []string{}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatalf("Error while getting blocks: %v", err)
	}
	for _, f := range files {
		if _, err := ulid.Parse(f.Name()); err == nil {
			blocks = append(blocks, f.Name())	
		}
	}
	return &blocks
}

func GetFilesInBlock(block string) *[]string {
	files := []string{}
	err := filepath.Walk(block,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files = append(files, path)
			}
			return nil
		})
	if err != nil {
		log.Fatalf("Error while getting files: %v", err)
	}
	return &files
}
