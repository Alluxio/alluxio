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
metrics_file_prefix=""
metrics_dir="/tmp/alluxio_metrics"

# Misc. arguments
time_step=2  # 2s is the default interval for `watch`
watch=""
jq_filter=""

USAGE="This script is used as a convenience method for monitoring &
periodically capturing the HTTP JSON sink for Alluxio metrics.

Usage:
Required:
  -H hostname              the host of the metrics HTTP server
  -p port                  the port of the metrics HTTP server
  -f metrics_file_prefix   the prefix prepended to the output metrics files

Optional:
  -w [WDIFF_OPTS]          setting this flag will use 'watch' to display the output
                           in the console, and appends the results to a single file
  -d metrics_dir           the directory to output the metrics files to (default: ${metrics_dir})
  -n time_step             the time in seconds between iterations (default: ${time_step}s)
  -F filter                a 'jq' compatible filter for filtering metrics
                           - eg: '.gauges."Cluster.BytesReadDirectThroughput"'

Misc:
  -h                         display this message


WDIFF_OPTS is required, and is one of:
  permanent      will run 'watch --differences=permanent'
  successive     will run 'watch --differences'
  none           will run 'watch' without '--differences'
"

# argument parsing
OPTIND=1
while getopts ":hH:d:p:f:F:n:w:" opt; do
    case ${opt} in
        d )
            metrics_dir="${OPTARG}"
            ;;
        f )
            metrics_file_prefix="${OPTARG}"
            ;;
        F ) jq_filter="${OPTARG}"
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
        w )
            watch="${OPTARG}"
            ;;
        * )
            echo "ERROR: Unknown argument"
            echo "${USAGE}"
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

# argument checking
if [[ -z "${host}" || -z "${port}" || -z "${metrics_file_prefix}" || -z "${metrics_dir}" ]]; then
    echo "ERROR: Must provide non-empty strings for arguments"
    echo "${USAGE}"
    exit 1
fi

# check if command `nslookup` exists
if ! command -v nslookup > /dev/null 2>&1; then
  echo "ERROR: nslookup command not found"
  exit 1
fi

# check if command `jq` exists
if ! command -v jq > /dev/null 2>&1; then
  echo "ERROR: jq command not found"
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

if [[ ! ( -z ${watch} ) ]]; then
    case ${watch} in
        "permanent" )   ;;
        "successive" )  ;;
        "none" )        ;;
        * )
            echo "ERROR: Invalid WDIFF_OPTS - ${watch}"
            exit 1
            ;;
    esac
fi

function main() {
    mkdir -p "${metrics_dir}"

    echo "Collecting metrics from http://${host}:${port}/metrics/json/ to ${metrics_dir}"
    echo "Press CTRL-C to stop collecting..."

    # trap ctrl-c and call cleanup()
    trap cleanup INT
    function cleanup() {
        echo -e "\nMetrics collected from http://${host}:${port}/metrics/json/ to ${metrics_dir}"
        exit 0
    }

    # We need to surround jq_filter with literal single-quotes
    # since we have special characters (i.e: '.') in our key names
    # - Method used: https://stackoverflow.com/a/1315213
    jq_filter=$(printf $'\'%s\'' "${jq_filter}")

    if [[ ${watch} ]]; then
        # default behaviour is to have no --differences flag
        differences_flag=""
        case "${watch}" in
            "permanent" )
                differences_flag='--differences=permanent'
                ;;
            "successive" )
                differences_flag='--differences'
                ;;
        esac

        # one-liner which appends to a single file
        watch ${differences_flag} --no-title --interval=${time_step} "curl --silent -XGET \"http://${host}:${port}/metrics/json/\" | jq ${jq_filter} | tee \"${metrics_dir}/${metrics_file_prefix}.json\""
    else
        # loop which outputs to new files for each iteration
        i=0
        while true; do
            metrics_file="${metrics_dir}/${metrics_file_prefix}.${i}.json"

            # need to wrap this with `bash -c` due to quoting issues with ${jq_filter}
            bash -c "curl --silent -XGET \"http://${host}:${port}/metrics/json/\" | jq ${jq_filter}" > "${metrics_file}"
            echo "Collected metrics to ${metrics_file}"
            i=$((i+1))
            sleep ${time_step}
        done
    fi
}

main
