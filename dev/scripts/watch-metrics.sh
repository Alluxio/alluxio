#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

# This script is analogous to `alluxio collectInfo collectMetrics` in that
# it periodically collects the output from the Alluxio HTTP JSON metrics sink.
# However you may also use it as a way to monitor differences in metrics
# over time due to the implementation using `watch`.
# - You can filter which specific metric(s) to collect via `jq` filters
#
# This process will output a new file containing the metrics at each
# polling interval into the specified metrics directory.

# Alluxio-related arguments
host=""
port=""
metrics_filename=""
metrics_dir="/tmp/alluxio_metrics"

# Misc. arguments
time_step=2  # 2s is the default interval for `watch`
watch=""
filter=""

USAGE="This script is used as a convenience method for monitoring &
periodically capturing the HTTP JSON sink for Alluxio metrics.
Metric names and descriptions are available at
https://docs.alluxio.io/os/user/stable/en/reference/Metrics-List.html

Usage:
Required:
  -H hostname              the host of the metrics HTTP server
  -p port                  the port of the metrics HTTP server

Optional:
  -f metrics_filename      the name of the output metrics file(s)
  -d [WDIFF_OPTS]          set this flag to control diff highlighting in 'watch'
  -n time_step             the time in seconds between iterations (default: ${time_step}s)

  -F filter                a JSON array of metric names/prefixes to filter on
                           - eg: '["Master.Rocks", "Master.TotalRpcs"]'

Misc:
  -h                       display this message


WDIFF_OPTS is required, and is one of:
  permanent      will run 'watch --differences=permanent'
  successive     will run 'watch --differences'
"

# argument parsing
OPTIND=1
while getopts ":hH:p:f:F:n:d:" opt; do
    case ${opt} in
        f )
            metrics_filename="${OPTARG}"
            ;;
        F ) filter="${OPTARG}"
            ;;
        h )
            echo "${USAGE}"
            exit 0
            ;;
        H )
            host="${OPTARG}"
            ;;
        n )
            time_step=${OPTARG}
            ;;
        p )
            port=${OPTARG}
            ;;
        d )
            diffs="${OPTARG}"
            ;;
        * )
            echo "ERROR: Unknown argument"
            echo "${USAGE}"
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

# check if command `nslookup` exists, required for curl
if ! command -v nslookup > /dev/null 2>&1; then
  echo "ERROR: nslookup command not found"
  exit 1
fi

# check if command `jq` exists
if ! command -v jq > /dev/null 2>&1; then
  echo "ERROR: jq command not found"
  exit 1
fi

# argument checking
if [[ -z "${host}" || -z "${port}" ]]; then
    echo "ERROR: Must provide non-empty strings for arguments"
    echo "${USAGE}"
    exit 1
fi

if [[ ${port} -le 0 ]]; then
    echo "ERROR: Port must be a positive integer"
    exit 1
fi

if [[ ${time_step} -le 0 ]]; then
    echo "ERROR: Time interval must be a positive integer"
    exit 1
fi

if [[ ! ( -z ${diffs} ) ]]; then
    case ${diffs} in
        "permanent" )   ;;
        "successive" )  ;;
        * )
            echo "ERROR: Invalid WDIFF_OPTS - ${diffs}"
            exit 1
            ;;
    esac
fi

if [[ ! ( -z "${metrics_filename}" ) ]]; then
    # attempt to create the file at the start,
    # will propagate permission/filepath errors to user

    # TODO(czhu): add flags to control whether or not to separate output into
    #             individual files, whether to truncate the file first
    touch "${metrics_filename}"
fi

function main() {
    echo "Collecting metrics from http://${host}:${port}/metrics/json/ to ${metrics_filename}"
    echo "Press CTRL-C to stop collecting..."

    # trap ctrl-c and call cleanup()
    trap cleanup INT
    function cleanup() {
        echo -e "\nMetrics collected from http://${host}:${port}/metrics/json/ to ${metrics_filename}"
        exit 0
    }

    # This cmd feeds the output of the curl request into jq to flatten the metrics
    # from different sources (gauges, counters, meters, and timers) into a single array.
    cmd="curl --silent -XGET \"http://${host}:${port}/metrics/json/\" | jq '[.gauges, .counters, .meters, .timers] | reduce .[] as \$metric ({}; . + \$metric)'"

    # if a filter is provided, filter the metrics by their key name
    if [[ ! ( -z "${filter}" ) ]]; then
        select_keys=""
        for key in `echo "${filter}" | jq -r '. | join(" ")'`; do
            echo "Parsed metric key: ${key}"
            if [[ ! ( -z $select_keys ) ]]; then
                select_keys+=" or "
            fi
            # using `startswith` since some of our metrics append metadata into the key name
            # eg: Worker.BlocksCached.<WORKER_IP>
            select_keys+="(.key | startswith(\"${key}\"))"
        done
        cmd+=" | jq 'with_entries(select( ${select_keys} ))'"
    fi

    # determine the differences flag to pass to `diffs`
    differences_flag=""
    case ${diffs} in
        "permanent" )
            differences_flag='--differences=permanent'
            ;;
        "successive" )
            differences_flag='--differences'
            ;;
    esac

    # one-liner which appends to a single file
    watch ${differences_flag} --no-title --interval=${time_step} "${cmd} | tee -a \"${metrics_filename}\""
}

main
