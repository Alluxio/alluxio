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

package quorum

import (
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/cli/env"
)

var (
	Service = &env.Service{
		Name:        "quorum",
		Description: "Manage the high availability server quorum, such as electing a new leader",
		Commands: []env.Command{
			Elect,
			Info,
			Remove,
		},
	}
	domains = map[string]struct{}{
		"MASTER":     {},
		"JOB_MASTER": {},
	}
)

func checkDomain(domain string) error {
	if _, ok := domains[domain]; !ok {
		var names []string
		for d := range domains {
			names = append(names, d)
		}
		return stacktrace.NewError("domain must be one of %v", strings.Join(names, ", "))
	}
	return nil
}
