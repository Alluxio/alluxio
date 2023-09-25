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

package util

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"
	"github.com/palantir/stacktrace"
)

func PrintProcessedJsonBytes(jsonInput []byte, raw bool) error {
	obj := orderedmap.New()
	if err := obj.UnmarshalJSON(jsonInput); err != nil {
		return stacktrace.Propagate(err, "error unmarshalling json from java command")
	}

	if !raw {
		newObj := orderedmap.New()
		for _, key := range obj.Keys() {
			if val, ok := obj.Get(key); ok {
				k, v := processKeyValuePair(key, val)
				newObj.Set(k, v)
			}
		}
		obj = newObj
	}

	prettyJson, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return stacktrace.Propagate(err, "error marshalling json to pretty format")
	}
	os.Stdout.Write(append(prettyJson, '\n'))
	return nil
}

func processKeyValuePair(key string, data interface{}) (string, interface{}) {
	switch value := data.(type) {
	case float64:
		if strings.HasSuffix(key, "Time") {
			return strings.TrimSuffix(key, "Time"), convertMsToDatetime(value)
		} else if strings.HasSuffix(key, "Duration") {
			return strings.TrimSuffix(key, "Duration"), convertMsToDuration(value)
		} else if strings.HasSuffix(key, "Bytes") {
			return strings.TrimSuffix(key, "Bytes"), convertBytesToString(value)
		} else {
			return key, value
		}
	case []interface{}:
		array := make([]interface{}, len(value))
		for i, item := range value {
			_, processedItem := processKeyValuePair(key, item)
			array[i] = processedItem
		}
		return key, array
	case orderedmap.OrderedMap:
		processedMap := orderedmap.New()
		for _, key := range value.Keys() {
			if val, ok := value.Get(key); ok {
				k, v := processKeyValuePair(key, val)
				processedMap.Set(k, v)
			}
		}
		return key, processedMap
	default:
		return key, value
	}
}

func convertMsToDatetime(timeMs float64) string {
	tm := time.Unix(int64(timeMs)/1000, 0)
	return tm.Format(time.RFC3339)
}

func convertMsToDuration(timeMs float64) string {
	duration := time.Duration(int64(timeMs)) * time.Millisecond
	days := duration / (24 * time.Hour)
	duration = duration % (24 * time.Hour)
	hours := duration / time.Hour
	duration = duration % time.Hour
	minutes := duration / time.Minute
	seconds := duration % time.Minute / time.Second
	return fmt.Sprintf("%dd %02dh%02dm%02ds", days, hours, minutes, seconds)
}

func convertBytesToString(bytes float64) string {
	const unit = 1024
	suffixes := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	exp, n := 0, int64(bytes)
	for n > 5*unit && exp < len(suffixes)-1 {
		n /= unit
		exp++
	}
	value := bytes / math.Pow(unit, float64(exp))
	size := strconv.FormatFloat(value, 'f', 2, 64)
	return size + suffixes[exp]
}
