package tool

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func FindFiles(dirPath string, suffix string) ([]string, error) {
	dirStat, err := os.Stat(dirPath)
	if err != nil {
		return nil, err
	}
	if !dirStat.Mode().IsDir() {
		return nil, errors.New(fmt.Sprintf("%s is not a directory", dirPath))
	}
	if err != nil {
		return nil, err
	}
	var files []string

	err = filepath.WalkDir(dirPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), suffix) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func ReplaceFileNames(filePaths []string, target string, replacement string) error {
	for _, oldPath := range filePaths {
		_, fileName := filepath.Split(oldPath)
		if strings.Contains(fileName, target) {
			fileName = strings.ReplaceAll(fileName, target, replacement)
			newPath := filepath.Join(filepath.Dir(oldPath), fileName)
			err := os.Rename(oldPath, newPath)
			if err != nil {
				fmt.Printf("Failed to rename file %s: %v\n", oldPath, err)
			} else {
				fmt.Printf("File %s renamed to %s\n", oldPath, newPath)
			}
		}
	}
	return nil
}

// ReadFile 读取指定路径的文件内容，并以字符串形式返回
func ReadFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// RenameFile 根据给定的路径和目标名称，将指定文件重命名为该名称
func RenameFile(filePath string, targetName string) error {
	dirPath := filepath.Dir(filePath)
	newPath := filepath.Join(dirPath, targetName)

	// 检查目标文件是否已存在
	_, err := os.Stat(newPath)
	if err == nil {
		return os.ErrExist
	}

	// 重命名文件
	err = os.Rename(filePath, newPath)
	if err != nil {
		return err
	}

	return nil
}
