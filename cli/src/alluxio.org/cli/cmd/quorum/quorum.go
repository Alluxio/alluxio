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
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

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
)

const (
	DomainJobMaster = "JOB_MASTER"
	DomainMaster    = "MASTER"
)

type QuorumCommand struct {
	*env.BaseJavaCommand

	AllowedDomains []string
	Domain         string
}

func (c *QuorumCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *QuorumCommand) InitQuorumCmd(cmd *cobra.Command) *cobra.Command {
	const domain = "domain"
	cmd.Flags().StringVar(&c.Domain, domain, "", fmt.Sprintf("Quorum domain to operate on, must be one of %v", strings.Join(c.AllowedDomains, ", ")))
	if err := cmd.MarkFlagRequired(domain); err != nil {
		panic(err)
	}
	return cmd
}

func (c *QuorumCommand) Run(args []string) error {
	if err := checkDomain(c.Domain, c.AllowedDomains...); err != nil {
		return stacktrace.Propagate(err, "error checking domain %v", c.Domain)
	}
	var javaArgs []string
	javaArgs = append(javaArgs, args...)
	javaArgs = append(javaArgs, "-domain", c.Domain)
	return c.Base().Run(javaArgs)
}

func checkDomain(domain string, allowedDomains ...string) error {
	for _, d := range allowedDomains {
		if domain == d {
			return nil
		}
	}
	return stacktrace.NewError("domain is %v but must be one of %v", domain, strings.Join(allowedDomains, ","))
}
