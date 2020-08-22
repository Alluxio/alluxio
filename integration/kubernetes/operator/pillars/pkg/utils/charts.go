package utils

import (
	"os"
)

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

var chartFolder = ""

// Get the directory of charts
func GetChartsDirectory() string {
	if chartFolder != "" {
		return chartFolder
	}
	homeChartsFolder := os.Getenv("HOME") + "/charts"
	if !PathExists(homeChartsFolder) {
		chartFolder = "/charts"
	} else {
		chartFolder = homeChartsFolder
	}
	return chartFolder
}
