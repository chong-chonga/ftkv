package tool

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const key = "do" // 加密秘钥

func main() {
	// 指定目录路径
	dirPath := "/path/to/your/directory"

	// 加密文件
	err := encryptFilesInDirectory(dirPath)
	if err != nil {
		fmt.Println("加密失败:", err)
		return
	}

	// 解密文件
	err = decryptFilesInDirectory(dirPath)
	if err != nil {
		fmt.Println("解密失败:", err)
		return
	}

	fmt.Println("加密解密成功！")
}

func encryptFilesInDirectory(dirPath string) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果是文件且扩展名为.jpg
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".jpg") {
			// 读取文件内容
			content, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			// 加密文件内容
			encryptedContent, err := encrypt(content, []byte(key))
			if err != nil {
				return err
			}

			// 写入加密后的内容
			err = ioutil.WriteFile(path, encryptedContent, 0644)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func decryptFilesInDirectory(dirPath string) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果是文件且扩展名为.jpg
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".jpg") {
			// 读取加密后的文件内容
			encryptedContent, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			// 解密文件内容
			decryptedContent, err := decrypt(encryptedContent, []byte(key))
			if err != nil {
				return err
			}

			// 写入解密后的内容
			err = ioutil.WriteFile(path, decryptedContent, 0644)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// 加密函数
func encrypt(data []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	encryptedData := gcm.Seal(nonce, nonce, data, nil)
	return encryptedData, nil
}

// 解密函数
func decrypt(encryptedData []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedData) < nonceSize {
		return nil, errors.New("加密数据太短")
	}

	nonce, encryptedData := encryptedData[:nonceSize], encryptedData[nonceSize:]
	decryptedData, err := gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, err
	}

	return decryptedData, nil
}
