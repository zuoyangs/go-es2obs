package elasticsearch

import "strings"

// filterIndices 根据指定条件筛选索引
func filterIndices(indexMap map[string]interface{}) []string {

	filteredIndices := make([]string, 0)
	for index := range indexMap {
		// 根据您的筛选条件进行逻辑判断
		if (strings.Contains(index, "jenkins") || strings.Contains(index, "2023")) && !strings.HasPrefix(index, ".") {
			filteredIndices = append(filteredIndices, index)
		}
	}
	return filteredIndices
}
