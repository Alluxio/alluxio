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

	debugFlag  bool
)

func init() {
	Root.Flags.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")
}
