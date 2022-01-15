package keyvalue

import "io/ioutil"

func listDir(path string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, file := range fileInfo {
		files = append(files, file.Name())
	}
	return files, nil
}
