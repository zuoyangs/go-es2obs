package elasticsearch

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/spf13/viper"
)

type ESConfig struct {
	Addresses []string
	Username  string
	Password  string
	Transport http.RoundTripper
	Logger    estransport.Logger
}

func NewElasticsearchClient() (*elasticsearch.Client, error) {
	// 读取配置文件
	viper.SetConfigFile("../etc/esConfig.yaml")

	err := viper.ReadInConfig()
	if err != nil {
		// 处理配置文件读取错误
		return nil, err
	}

	// 创建新的 Elasticsearch client 配置
	var cfg ESConfig
	err = viper.Unmarshal(&cfg) // 将配置文件中的值填充到 cfg
	if err != nil {
		// 处理配置文件解析错误
		return nil, err
	}

	// 设置默认的 Transport 配置
	if cfg.Transport == nil {
		cfg.Transport = &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		}
	}

	// 创建 Elasticsearch client
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: cfg.Transport,
		Logger:    cfg.Logger,
	})
	if err != nil {
		// 处理创建 client 错误
		return nil, err
	}

	return esClient, nil
}

// getAllIndices 从 Elasticsearch 获取所有索引
func getAllIndices(client *es.Client) (map[string]interface{}, error) {
	res, err := client.Indices.Get([]string{"_all"})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error response: %s", res.String())
	}

	var indexMap map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&indexMap)
	if err != nil {
		return nil, err
	}

	return indexMap, nil
}
