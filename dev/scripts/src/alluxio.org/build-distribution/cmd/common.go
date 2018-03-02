package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const versionMarker = "${VERSION}"

// hadoopDistributions maps hadoop distributions to versions
var hadoopDistributions = map[string]version{
	"hadoop-1.0": parseVersion("1.0.4"),
	"hadoop-1.2": parseVersion("1.2.1"),
	"hadoop-2.2": parseVersion("2.2.0"),
	"hadoop-2.3": parseVersion("2.3.0"),
	"hadoop-2.4": parseVersion("2.4.1"),
	"hadoop-2.5": parseVersion("2.5.2"),
	"hadoop-2.6": parseVersion("2.6.5"),
	"hadoop-2.7": parseVersion("2.7.3"),
	"hadoop-2.8": parseVersion("2.8.0"),
}

func validHadoopDistributions() []string {
	var result []string
	for distribution := range hadoopDistributions {
		result = append(result, distribution)
	}
	sort.Strings(result)
	return result
}

func run(desc, cmd string, args ...string) string {
	fmt.Printf("  %s ... ", desc)
	if debugFlag {
		fmt.Printf("\n    command: %s %s ... ", cmd, strings.Join(args, " "))
	}
	c := exec.Command(cmd, args...)
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	c.Stderr = stderr
	c.Stdout = stdout
	if err := c.Run(); err != nil {
		fmt.Printf("\"%v %v\" failed: %v\nstderr: <%v>\nstdout: <%v>\n", cmd, strings.Join(args, " "), err, stderr.String(), stdout.String())
		os.Exit(1)
	}
	fmt.Println("done")
	return stdout.String()
}
