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

package info

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Report = &ReportCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "report",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"report"},
	},
}

type ReportCommand struct {
	*env.BaseJavaCommand
	raw bool
}

func (c *ReportCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ReportCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [arg]", c.CommandName),
		Short: "Reports Alluxio running cluster information",
		Long: `Reports Alluxio running cluster information
[arg] can be one of the following values:
  jobservice: job service metrics information
  metrics:    metrics information
  summary:    cluster summary
  ufs:        under storage system information

Defaults to summary if no arg is provided
`,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.raw, "raw", false,
		"Output raw JSON data instead of human-readable format for bytes, datetime, and duration.")
	return cmd
}

func (c *ReportCommand) Run(args []string) error {
	reportArg := "summary"
	if len(args) == 1 {
		options := map[string]struct{}{
			"jobservice": {},
			"metrics":    {},
			"summary":    {},
			"ufs":        {},
		}
		if _, ok := options[args[0]]; !ok {
			var cmds []string
			for c := range options {
				cmds = append(cmds, c)
			}
			return stacktrace.NewError("first argument must be one of %v", strings.Join(cmds, ", "))
		}
		reportArg = args[0]
	}

	buf := &bytes.Buffer{}
	if err := c.RunWithIO([]string{reportArg}, nil, buf, os.Stderr); err != nil {
		io.Copy(os.Stdout, buf)
		return err
	}

	obj := orderedmap.New()
	if err := obj.UnmarshalJSON(buf.Bytes()); err != nil {
		return stacktrace.Propagate(err, "error unmarshalling json from java command")
	}

	if !c.raw {
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
