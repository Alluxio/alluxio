/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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
