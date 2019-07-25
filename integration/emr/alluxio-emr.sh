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

# This script is meant for bootstrapping the Alluxio service to an EMR cluster.
# Arguments for the script are listed below.
# Arg 1. Download URI (ex. http://downloads.alluxio.io/downloads/files/2.0.0/alluxio-2.0.0-bin.tar.gz)
# Arg 2. Root UFS URI (ex. s3://my-bucket/alluxio-emr/mount)
# Arg 3. (Optional) HTTP(S) or S3 URIs pointing to alluxio-site.properties file
#        that will be used on cluster startup. The resource must be named "alluxio-site.properties"
# Arg 4. (Optional) Extra Alluxio Options. These will be appended to
#        alluxio-site.properties. Multiple options can be specified using Arg 5
#        as a delimiter (ex. alluxio.user.file.writetype.default=CACHE_THROUGH;alluxio.user.file.readtype.default=CACHE)
# Arg 5. (Optional) Delimeter for additional properties. Defaults to ;

set -o errexit  # exit when a command fails - append "|| true" to allow a
                # command to fail
set -o nounset  # exit when attempting to use undeclared variables

# Show commands being run - useful to keep by default so that failures
# are easy to debug through AWS
set -x

# script constants
ALLUXIO_HOME=/opt/alluxio
ALLUXIO_SITE_PROPERTIES=${ALLUXIO_HOME}/conf/alluxio-site.properties

# Downloads a file to the local machine from a remote HTTP(S) or S3 URI into the cwd
#
# Args:
#   $1: URI - the remote location to retrieve the file from
download_file() {
  local uri=$1

  if [[ "${uri}" == s3://* ]]
  then
    aws s3 cp ${uri} ./
  else
    # TODO Add metadata header tag to the wget for filtering out in download metrics.
    wget -nv "${uri}"
  fi
}

# Run a command as a specific user
# Assumes the provided user already exists on the system and user running script has sudo access
#
# Args:
#   $1: user - the username to run the command as
#   $2: command - the command to run
doas() {
  local user=$1
  local cmd=$2
  sudo runuser -l ${user} -c "${cmd}"
}

# Appends a property KV pair to the alluxio-site.properties file
#
# Args:
#   $1: property name
#   $2: property value
append_alluxio_property() {
  local property=$1
  local value=$2

  # OK to fail in this section
  set +o errexit
  # /opt/alluxio/conf must exist for this to work
  grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null
  local rv=$?
  set -o errexit # errors not ok anymore
  echo $rv
  if [ $rv -ne 0 ]; then
    echo "append prop"
    doas alluxio "echo '${property}=${value}' >> ${ALLUXIO_SITE_PROPERTIES}"
  fi
}

# Calculates the default memory size as 1/3 of the total system memory
#
# Echo's the result to stdout. To store the return value in a variable use
# val=$(get_defaultmem_size)
get_default_mem_size() {
  local mem_div=3
  local phy_total=$(free -m | grep -oP '\d+' | head -n1)
  local mem_size=$(( ${phy_total} / ${mem_div} ))
  echo "${mem_size}MB"
}

# Gets the region of the current EC2 instance
get_aws_region() {
  local ec2_avail_zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)
  local ec2_region="$(echo "${ec2_avail_zone}" | sed 's/[a-z]$//')"
  echo "${ec2_region}"
}

main() {
  local alluxio_tarball
  local root_ufs_uri
  local site_properties_uri
  local property_delimeter
  local delimited_properties
  alluxio_tarball=${1}
  root_ufs_uri=${2}
  site_properties_uri=${3:-""}
  delimited_properties=${4:-""}
  property_delimeter=${5:-";"}

  # Create user
  sudo groupadd alluxio -g 600
  sudo useradd alluxio -u 600 -g 600

  # Download the release
  if [[ -z ${alluxio_tarball} ]]
  then
    echo "No Download URL Provided. Please go to http://downloads.alluxio.io to see available release downloads."
  else
    download_file ${alluxio_tarball}
  fi

  local release=`basename ${alluxio_tarball}`
  local release_unzip=${release%"-bin.tar.gz"}
  release_unzip=${release_unzip%".tar.gz"}

  # Unpack and inflate the release tar
  # TODO logic for different compression formats, s3 URIs, git URIs, etc.
  sudo cp ${release} /opt/
  sudo tar -xvf /opt/${release} -C /opt/
  sudo rm -R /opt/${release}
  sudo mv /opt/${release_unzip} ${ALLUXIO_HOME}
  sudo chown -R alluxio:alluxio ${ALLUXIO_HOME}
  rm ${release}
  doas alluxio "cp ${ALLUXIO_SITE_PROPERTIES}.template ${ALLUXIO_SITE_PROPERTIES}"

  local aws_region=$(get_aws_region)

  #Get hostnames and load into masters/workers file
  local emr_cluster=$(jq '.jobFlowId' /mnt/var/lib/info/job-flow.json | sed -e 's/^"//' -e 's/"$//')
  local hostlist=$(aws emr list-instances --cluster-id ${emr_cluster} --region ${aws_region} | jq '.Instances[].PrivateDnsName' | sed -e 's/^"//' -e 's/"$//')

  # Should succeed only on workers. Otherwise, var is left empty
  local master=$(jq '.masterHost' /mnt/var/lib/info/extraInstanceData.json | sed -e 's/^"//' -e 's/"$//' | nslookup | awk '/name/{print substr($NF,1,length($NF)-1)}')

  # Logic to get master hostname if on the master
  if [[ -z "${master}" ]]
  then
    master=`hostname`
    master=${master}".ec2.internal"
  fi

  local workers=`printf '%s\n' "${hostlist//$master/}"`

  doas alluxio "echo '${master}' > ${ALLUXIO_HOME}/conf/masters"
  doas alluxio "echo '${workers}' > ${ALLUXIO_HOME}/conf/workers"

  # Identify master
  local is_master=`jq '.isMaster' /mnt/var/lib/info/instance.json`

  # Download user-specified site properties file to
  # ${ALLUXIO_HOME/conf/alluxio-site.properties}. Must be named
  # "alluxio-site.properties"
  if [ ! -z "${site_properties_uri}" ]; then
    download_file ${site_properties_uri}
    mv ./alluxio-site.properties /tmp/alluxio-site.properties
    sudo chown alluxio:alluxio /tmp/alluxio-site.properties
    doas alluxio "cp /tmp/alluxio-site.properties ${ALLUXIO_SITE_PROPERTIES}"
    # Add newline in case file doens't end in newline
    doas alluxio "echo >> ${ALLUXIO_SITE_PROPERTIES}"
    sudo rm /tmp/alluxio-site.properties
  fi

  # Inject user defined properties from args
  IFS="${property_delimeter}"
  conf=(${delimited_properties})
  for property in ${conf}; do
    IFS="=" read key value <<< ${property}
    append_alluxio_property ${key} ${value}
  done

  local mem_size=$(get_default_mem_size)

  # Append default configs to site properties if the user hasn't set them
  # already
  append_alluxio_property alluxio.master.hostname "${master}"
  append_alluxio_property alluxio.master.journal.type "UFS"
  append_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"
  append_alluxio_property alluxio.master.security.impersonation.hive.users "*"
  append_alluxio_property alluxio.master.security.impersonation.presto.users "*"
  append_alluxio_property alluxio.master.security.impersonation.yarn.users "*"
  append_alluxio_property alluxio.worker.memory.size "${mem_size}"
  append_alluxio_property alluxio.worker.tieredstore.level0.alias "MEM"
  append_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "/mnt/ramdisk"
  append_alluxio_property alluxio.worker.tieredstore.levels "1"

  # Alluxio can't rely on SSH to start services (i.e. no alluxio-start.sh all)
  if [[ ${is_master} = "true" ]]
  then
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a master"
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a job_master"
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a proxy"
  else
    ${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount local
    until ${ALLUXIO_HOME}/bin/alluxio fsadmin report
    do
      sleep 5
    done
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a worker"
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a job_worker"
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a proxy"
  fi

  # Compute application configs
  doas alluxio "ln -s ${ALLUXIO_HOME}/client/*client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar"
  sudo mkdir -p /usr/lib/spark/jars/
  sudo ln -s ${ALLUXIO_HOME}/client/alluxio-client.jar /usr/lib/spark/jars/alluxio-client.jar
  sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
  sudo ln -s ${ALLUXIO_HOME}/client/alluxio-client.jar /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar
}

main "$@"
