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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

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
	format string
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
	cmd.Flags().StringVar(&c.format, "format", "json",
		"Set output format, any of [json, yaml]")
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
	// TODO: output all in a serializable format and filter/trim as specified by flags
	if c.format != "json" && c.format != "yaml" {
		return stacktrace.NewError("unfamiliar output format %s, must be one of [json, yaml]", c.format)
	}
	buf := &bytes.Buffer{}
	if err := c.RunWithIO([]string{reportArg}, nil, buf, os.Stderr); err != nil {
		io.Copy(os.Stdout, buf)
		return err
	}

	obj := orderedmap.New()
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		return stacktrace.Propagate(err, "error unmarshalling json from java command")
	}

	if reportArg == "summary" {
		if val, ok := obj.Get("freeCapacity"); ok {
			obj.Set("freeCapacity", convertBytesToString(val.(float64)))
		}
		if val, ok := obj.Get("started"); ok {
			obj.Set("started", convertMsToDatetime(val.(float64)))
		}
		if val, ok := obj.Get("uptime"); ok {
			obj.Set("uptime", convertMsToDuration(val.(float64)))
		}
		if val, ok := obj.Get("totalCapacityOnTiers"); ok {
			oMap := val.(orderedmap.OrderedMap)
			for key := range oMap.Values() {
				if val, ok := oMap.Get(key); ok {
					oMap.Set(key, convertBytesToString(val.(float64)))
				}
			}
		}
		if val, ok := obj.Get("usedCapacityOnTiers"); ok {
			oMap := val.(orderedmap.OrderedMap)
			for key := range oMap.Values() {
				if val, ok := oMap.Get(key); ok {
					oMap.Set(key, convertBytesToString(val.(float64)))
				}
			}
		}
	}

	if reportArg == "ufs" {
		for key := range obj.Values() {
			if val, ok := obj.Get(key); ok {
				oMap := val.(orderedmap.OrderedMap)
				if v, ok := oMap.Get("ufsCapacityBytes"); ok {
					oMap.Set("ufsCapacityBytes", convertBytesToString(v.(float64)))
				}
				if v, ok := oMap.Get("ufsUsedBytes"); ok {
					oMap.Set("ufsUsedBytes", convertBytesToString(v.(float64)))
				}
				obj.Set(key, oMap)
			}
		}
	}

	if reportArg == "jobservice" {
		if val, ok := obj.Get("masterStatus"); ok {
			for _, item := range val.([]interface{}) {
				oMap := item.(orderedmap.OrderedMap)
				if v, ok := oMap.Get("startTime"); ok {
					oMap.Set("startTime", convertMsToDatetime(v.(float64)))
				}
				item = oMap
			}
		}
		if val, ok := obj.Get("longestRunningJobs"); ok {
			for _, item := range val.([]interface{}) {
				oMap := item.(orderedmap.OrderedMap)
				if v, ok := oMap.Get("timestamp"); ok {
					oMap.Set("timestamp", convertMsToDatetime(v.(float64)))
				}
				item = oMap
			}
		}
		if val, ok := obj.Get("recentFailedJobs"); ok {
			for _, item := range val.([]interface{}) {
				oMap := item.(orderedmap.OrderedMap)
				if v, ok := oMap.Get("timestamp"); ok {
					oMap.Set("timestamp", convertMsToDatetime(v.(float64)))
				}
				item = oMap
			}
		}
		if val, ok := obj.Get("recentModifiedJobs"); ok {
			for _, item := range val.([]interface{}) {
				oMap := item.(orderedmap.OrderedMap)
				if v, ok := oMap.Get("timestamp"); ok {
					oMap.Set("timestamp", convertMsToDatetime(v.(float64)))
				}
				item = oMap
			}
		}
	}

	if c.format == "json" {
		prettyJson, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			return stacktrace.Propagate(err, "error marshalling json to pretty format")
		}
		os.Stdout.Write(append(prettyJson, '\n'))
	} else if c.format == "yaml" {
		out, err := yaml.Marshal(obj)
		if err != nil {
			return stacktrace.Propagate(err, "error marshalling yaml")
		}
		os.Stdout.Write(out)
	}
	return nil
}

func convertMsToDatetime(timeMs float64) string {
	tm := time.Unix(int64(timeMs)/1000, 0)
	format := "2006-01-02T15:04:05-07:00"
	return tm.Format(format)
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
	div, exp := 1, 0
	for n := int64(bytes); n > 5*unit; n /= unit {
		div *= unit
		exp++
	}
	value := bytes / float64(div)
	size := strconv.FormatFloat(value, 'f', 2, 64)
	suffixes := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	return size + suffixes[exp]
}
