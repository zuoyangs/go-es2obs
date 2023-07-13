package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/spf13/viper"
)

type OBSConfig struct {
	Addresses []string
	Username  string
	Password  string
	Transport http.RoundTripper
	Logger    estransport.Logger
}

type OBSClient struct {
	Client *obs.ObsClient
}

func NewOBSClient() (*OBSClient, error) {
	// 读取配置文件
	viper.SetConfigFile("../etc/obsConfig.yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %s", err)
	}

	// 从配置文件中获取OBS访问信息
	endpoint := viper.GetString("endpoint")
	accessKey := viper.GetString("accessKey")
	secretKey := viper.GetString("secretKey")

	// 创建OBS客户端实例
	obsClient, err := obs.New(endpoint, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create OBS client: %s", err)
	}

	return &OBSClient{
		Client: obsClient,
	}, nil
}

func backupIndices(indices []string, baseURL, repository, bucketName, folderName string, obsClient *OBSClient) error {
	for _, index := range indices {
		snapshotName := index

		err := createSnapshot(baseURL, repository, index, snapshotName)
		if err != nil {
			return fmt.Errorf("failed to create snapshot %s: %s", snapshotName, err)
		}

		err = uploadSnapshotToOBS(obsClient, snapshotName, folderName, bucketName)
		if err != nil {
			return fmt.Errorf("failed to upload snapshot %s to OBS: %s", snapshotName, err)
		}

		fmt.Printf("Snapshot %s is backed up to OBS\n", snapshotName)
	}

	return nil
}

func uploadSnapshotToOBS(obsClient *OBSClient, snapshotName, folderName, bucketName string) error {
	snapshotPayload := struct {
		Snapshot string `json:"snapshot"`
		Target   struct {
			Bucket string `json:"bucket"`
			Prefix string `json:"prefix"`
		} `json:"target"`
	}{
		Snapshot: snapshotName,
		Target: struct {
			Bucket string `json:"bucket"`
			Prefix string `json:"prefix"`
		}{
			Bucket: bucketName,
			Prefix: folderName + "/",
		},
	}
	snapshotPayloadBytes, err := json.Marshal(snapshotPayload)
	if err != nil {
		return fmt.Errorf("error marshaling snapshot payload: %s", err)
	}

	// Upload the snapshot to OBS
	uploadPath := folderName + "/" + snapshotName
	_, err = obsClient.PutObject(bucketName, uploadPath, bytes.NewReader(snapshotPayloadBytes))
	if err != nil {
		return fmt.Errorf("failed to upload snapshot %s to OBS: %s", snapshotName, err)
	}

	return nil
}
