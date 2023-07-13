package elasticsearch

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func CreateRepository(es *elasticsearch.Client) {
	req := esapi.SnapshotCreateRepositoryRequest{
		Repository: "my_backup",
		Body: strings.NewReader(`
			{
				"type" : "obs",
				"settings" : {
					"bucket" : "css-backup-name",
					"base_path" : "css_backup/711/",
					"chunk_size" : "2g",
					"endpoint" : "obs.xxx.huawei.com:443",
					"region" : "xxx",
					"compress" : "true",
					"access_key": "xxxxx",
					"secret_key": "xxxxxxxxxxxxxxxxx",
					"max_restore_bytes_per_sec": "100mb",
					"max_snapshot_bytes_per_sec": "100mb"
				}
			}
		`),
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error creating repository: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error creating repository: %s", res.String())
	}

	log.Println("Repository created successfully")
}

func CreateSnapshot(es *elasticsearch.Client) {
	snapshotName := time.Now().Format("2006-01-02")
	req := esapi.SnapshotCreateRequest{
		Repository: "my_backup",
		Snapshot:   snapshotName,
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error creating snapshot: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error creating snapshot: %s", res.String())
	}

	log.Printf("Snapshot '%s' created successfully", snapshotName)
}

func GetSnapshotStatus(es *elasticsearch.Client) {
	snapshotName := time.Now().Format("2006-01-02")
	req := esapi.SnapshotStatusRequest{
		Repository: []string{"my_backup"},
		Snapshot:   []string{snapshotName},
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting snapshot status: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error getting snapshot status: %s", res.String())
	}

	var respBody map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&respBody)
	if err != nil {
		log.Fatalf("Error decoding snapshot status response: %s", err)
	}

	status := respBody["snapshots"].([]interface{})[0].(map[string]interface{})["state"]
	log.Printf("Snapshot '%s' status: %s", snapshotName, status)
}
