package tool

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type RequestBody struct {
	Param string `json:"param"`
}

// HandlePostRequest 处理POST请求的处理函数
func HandlePostRequest(w http.ResponseWriter, r *http.Request) {
	// 读取请求体
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "无法读取请求体", http.StatusBadRequest)
		return
	}

	// 解析JSON
	var requestBody RequestBody
	err = json.Unmarshal(body, &requestBody)
	if err != nil {
		http.Error(w, "无法解析JSON", http.StatusBadRequest)
		return
	}

	// 在这里可以根据需要处理请求参数
	paramValue := requestBody.Param

	// 构建返回的字符串
	responseString := fmt.Sprintf("收到的参数值为：%s", paramValue)

	// 返回字符串
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(responseString))
}
