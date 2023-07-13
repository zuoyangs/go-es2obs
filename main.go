package main

import (
	"log"
)

func main() {

	// 从配置文件中获取配置，并创建 elasticsearch client
	esClient, err := NewElasticsearchClient()
	if err != nil {
		log.Fatalf("Error create elasticsearch client: %s", err)
	}

	// Get all indices
	indexMap, err := getAllIndices(esClient)
	if err != nil {
		log.Fatalf("Error getting indices: %s", err)
	}

	// Filter indices
	filteredIndices := filterIndices(indexMap)
	if err != nil {
		log.Fatalf("Error filteredIndices: %s", err)
	}

	CreateRepository(es)
	CreateSnapshot(es)
	GetSnapshotStatus(es)

	// 从配置文件中获取配置，并创建 obs client
	obsClient, err := NewOBSClient()
	if err != nil {
		log.Fatalf("Failed to create OBS client: %s", err)
	}

	_, err := backupIndices(filteredIndices, baseURL, repository, bucketName, folderName, obsClient*obsClient)

	_, b := uploadSnapshotToOBS(obsClient*obsClient, snapshotName, folderName, bucketName)

}
