package util

import "os"

func EnsureDirExists(path string) {
	if err := os.MkdirAll(path, 0744); err != nil {
		panic(err)
	}
}
