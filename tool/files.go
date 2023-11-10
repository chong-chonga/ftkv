package tool

import (
	"bufio"
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
			reader := bufio.NewReader(os.Stdin)
			fmt.Printf("%s -> %s Y/N ", oldPath, newPath)
			input, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			if strings.Contains(input, "Y") || strings.Contains(input, "y") {
				err := os.Rename(oldPath, newPath)
				if err != nil {
					fmt.Printf("Failed to rename file %s: %v\n", oldPath, err)
				} else {
					fmt.Printf("File %s renamed to %s\n", oldPath, newPath)
				}
			}
		}
	}
	return nil
}
