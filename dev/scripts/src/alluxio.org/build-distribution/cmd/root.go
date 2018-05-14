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

package cmd

import (
	"v.io/x/lib/cmdline"
)

var (
	Root = &cmdline.Command{
		Name:  "generate-tarballs",
		Short: "tool for creating alluxio tarballs",
		Long: `
	The publish tool contains functionality for generating either a single alluxio tarball,
or generating a suite of release tarballs.
	`,
		Children: []*cmdline.Command{
			cmdSingle,
			cmdRelease,
		},
	}

        callHomeFlag       bool
        callHomeBucketFlag string

	debugFlag bool

        proxyURLFlag   string
)

func init() {
	Root.Flags.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")

        // Call home
        Root.Flags.BoolVar(&callHomeFlag, "call-home", true, "whether the generated distributions should perform call home")
        Root.Flags.StringVar(&callHomeBucketFlag, "call-home-bucket", "", "the S3 bucket the generated distribution should upload call home information to")

        Root.Flags.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for sending diagnostic information")
}
