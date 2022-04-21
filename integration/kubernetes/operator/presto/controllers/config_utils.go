/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"hash/fnv"
	"strings"
)

func ConfigDataHash(config map[string]string) uint32 {
	var hashcode uint32 = 0
	for key, value := range config {
		hashcode ^= hashKeyValuePair(key, value)
	}
	return hashcode
}

func hashKeyValuePair(key, value string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(strings.TrimSpace(key)))
	h.Write([]byte(strings.TrimSpace(value)))
	return h.Sum32()
}
