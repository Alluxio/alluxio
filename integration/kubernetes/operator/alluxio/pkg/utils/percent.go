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
	"fmt"
	"strconv"
	"strings"
)

// PercentOf - calculate what percent [number1] is of [number2].
// ex. 300 is 12.5% of 2400
func PercentOf(part int, total int) float64 {
	return (float64(part) * float64(100.00)) / float64(total)
}

// PercentOfFloat - calculate what percent [number1] is of [number2].
// ex. 300 is 12.5% of 2400
func PercentOfFloat(part float64, total float64) float64 {
	return (float64(part) * float64(100.00)) / float64(total)
}

// PercentFromString - Get the percentage for string.
// ex. 100 % to 100.00
func PercentFromString(percentStr string) (percent float64, err error) {
	array := strings.Fields(percentStr)
	if len(array) < 1 {
		return percent, fmt.Errorf("Failed to parse %s", percentStr)
	}

	percent, err = strconv.ParseFloat(array[0], 64)

	return
}
