package operations

import (
	"fmt"
	"strings"

	units "github.com/docker/go-units"
)

func (a AlluxioFileUtils) CachedState() (cached int64, err error) {
	var (
		command = []string{"alluxio", "fsadmin", "report"}
		stdout  string
		stderr  string
	)

	found := false
	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	str := strings.Split(stdout, "\n")

	for _, s := range str {
		if strings.Contains(s, "Used Capacity:") {
			values := strings.Fields(s)
			if len(values) == 0 {
				return cached, fmt.Errorf("Failed to parse %s", s)
			}
			cached, err = units.RAMInBytes(values[len(values)-1])
			if err != nil {
				return
			}
			found = true
		}
	}

	if !found {
		err = fmt.Errorf("Failed to find the cache in output %v", stdout)
	}

	return
}

func (a AlluxioFileUtils) CleanCache(path string) (err error) {
	var (
		command = []string{"alluxio", "fs", "free", "-f", path}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	return
}
