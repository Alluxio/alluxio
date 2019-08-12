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

set -o errexit  # exit when a command fails - append "|| true" to allow a
                # command to fail
set -o nounset  # exit when attempting to use undeclared variables

# Show commands being run - useful to keep by default so that failures
# are easy to debug through AWS
set -x

# script constants
ALLUXIO_HOME=/opt/alluxio
ALLUXIO_SITE_PROPERTIES=${ALLUXIO_HOME}/conf/alluxio-site.properties
AWS_SHUTDOWN_ACTIONS_DIR=/mnt/var/lib/instance-controller/public/shutdown-actions/
USAGE="Usage: alluxio-emr.sh <root-ufs-uri> [ -d <alluxio-download-uri>]
                             [-b <backup_uri>]
                             [-i <journal_backup_uri>]
                             [-p <delimited_properties>]
                             [-s <property_delimiter>]
                             [-f <file_uri>]

alluxio-emr.sh is a script which can be used to bootstrap an AWS EMR cluster
with Alluxio. It can download and install Alluxio as well as add properties
specified as arguments to the script.

  <root-ufs-uri>    (Required) The URI of the root UFS in the Alluxio
                    namespace.

  -b                An s3:// URI that the Alluxio master will write a backup
                    to upon shutdown of the EMR cluster. The backup and and
                    upload MUST be run within 60 seconds. If the backup cannot
                    finish within 60 seconds, then an incomplete journal may
                    be uploaded. This option is not recommended for production
                    or mission critical use cases where the backup is relied
                    upon to restore cluster state after a previous shutdown.

  -d                An s3:// or https:// URI which points to an Alluxio
                    tarball. If this argument isn't specified, it is assumed
                    that Alluxio is already installed under /opt/alluxio.
                    Otherwise this script will download and untar the given
                    tarball and install Alluxio at /opt/alluxio if an Alluxio
                    installation doesn't already exist at that location.

  -f                An s3:// or https:// URI to any remote file. This property
                    can be specified multiple times. Any file specified through
                    this property will be downloaded and stored with the same
                    name to /opt/alluxio/conf/

  -i                An s3:// or https:// URI which represent the URI of a
                    previous Alluxio journal backup. If supplied, the backup
                    will be downloaded, and upon Alluxio startup, the Alluxio
                    master will read and restore the backup.

  -p                A string containing a delimited set of properties which
                    should be added to the
                    ${ALLUXIO_HOME}/conf/alluxio-site.properties file. The
                    delimiter by default is a semicolon \";\". If a different
                    delimiter is desired use the [-s] argument.

  -s                A string containing a single character representing what
                    delimiter should be used to split the Alluxio properties
                    provided in the [-p] argument.
"

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
  # ${ALLUXIO_SITE_PROPERTIES} must exist for this to work
  grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null
  local rv=$?
  set -o errexit # errors not ok anymore
  if [[ $rv -ne 0 ]]; then
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


print_help() {
  if [[ "${1}" -eq "1" ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi
  echo -e "${USAGE}"
  exit ${1}

}

# Installs alluxio to /opt/alluxio
#
# Arguments:
#         1: The s3:// or http(s):// URI that points to an Alluxio tarball
install_alluxio() {
  local alluxio_tarball
  alluxio_tarball=${1}

  download_file ${alluxio_tarball}

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

}

# Puts a shutdown hook under the EMR defined
# /mnt/var/lib/instance-controller/public/shutdown-actions/ directory.
#
# Arguments:
#         1: The URI to upload the backup to. Requires Alluxio to still be running.
register_backup_on_shutdown() {

  local backup_uri
  backup_uri="${1}"
  mkdir -p "${AWS_SHUTDOWN_ACTIONS_DIR}"

  echo "#!/usr/bin/env bash

# This script will shut down, and then back up and upload the Alluxio journal to
# the S3 path ${AWS_SHUTDOWN_ACTIONS_DIR}. The path can then be used in
conjunction with the -i (restore from backup) option.

mkdir -p /tmp/alluxio_backups
cd /tmp/alluxio_backups
${ALLUXIO_HOME}/bin/alluxio fsadmin backup
aws s3 cp /tmp/alluxio_backups \"${backup_uri}\"

" > ${AWS_SHUTDOWN_ACTIONS_DIR}


}

main() {
  local backup_uri
  local root_ufs_uri
  local property_delimiter
  local delimited_properties
  local restore_from_backup_uri
  local files_list

  if [[ "$#" -lt "1" ]]; then
    echo -e "No root UFS URI provided"
    print_help 1
  fi

  root_ufs_uri=${1}
  shift

  backup_uri=""
  property_delimiter=";"
  restore_from_backup_uri=""
  delimited_properties=""
  files_list=""


  while getopts "c:d:f:i:p:s:" option; do
    case "${option}" in
      b)
        backup_uri=${OPTARG}
        ;;
      d)
        alluxio_tarball="${OPTARG}"
        ;;
      f)
        # URIs to http(s)/s3 URIs should be URL encoded, so a space delimiter
        # works without issue.
        files_list+=" ${OPTARG}"
        ;;
      i)
        restore_from_backup_uri="${OPTARG}"
        ;;
      p)
        delimited_properties="${OPTARG}"
        ;;
      s)
        property_delimiter="${OPTARG}"
        ;;
      *)
        print_help 1
        ;;
    esac
  done

  if [[ "${alluxio_tarball}" ]]; then
    alluxio_tarball="https://downloads.alluxio.io/downloads/files/2.0.0/alluxio-2.0.0-bin.tar.gz"
  fi


  if [[ ! -d "/opt/alluxio" ]]; then
    install_alluxio "${alluxio_tarball}"
  fi


  if [[ ! -d "/opt/alluxio" ]]; then
    echo -e "/opt/alluxio install not found. Please provide a download URI with
-d or install it on the OS before running this script."
    exit 1
  fi

  # Create user
  sudo groupadd alluxio -g 600
  sudo useradd alluxio -u 600 -g 600

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

  # Download files provided by "-f" to /opt/alluxio/conf
  local cwd=`pwd`
  cd ${ALLUXIO_HOME}/conf
  IFS=" " read -ra files_to_be_downloaded <<< "${files_list}"
  for file in "${files_to_be_downloaded[@]}"; do
    download_file "${file}"
  done
  sudo chown -R alluxio:alluxio /opt/alluxio/conf
  cd "${cwd}"

  # Add newline to alluxio-site.properties in case the provided file doesn't end in newline
  doas alluxio "echo >> ${ALLUXIO_SITE_PROPERTIES}"

  if [[ "${delimited_properties}" ]]; then
    # Inject user defined properties from args
    IFS="${property_delimiter}" read -ra conf <<< "${delimited_properties}"
    for property in "${conf[@]}"; do
      local key=${property%%"="*}
      local value=${property#*"="}
      append_alluxio_property "${key}" "${value}"
    done
  fi

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
    local args=""
    if [[ "${restore_from_backup_uri}" ]]; then
      local backup_name="$(basename ${restore_from_backup_uri})"
      mkdir -p /tmp/alluxio_backup
      aws s3 cp "${backup_name}" /tmp/alluxio_backup
      args="-i /tmp/alluxio_backup/${backup_name}"
    fi
    doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a ${args} master"
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

  # Wait until now to register the backup function because it won't complete
  # unless the master is running

  if [[ "${backup_uri}" ]]; then
    register_backup_on_shutdown "${backup_uri}"
  fi

  # Compute application configs
  doas alluxio "ln -s ${ALLUXIO_HOME}/client/*client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar"
  sudo mkdir -p /usr/lib/spark/jars/
  sudo ln -s ${ALLUXIO_HOME}/client/alluxio-client.jar /usr/lib/spark/jars/alluxio-client.jar
  sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
  sudo ln -s ${ALLUXIO_HOME}/client/alluxio-client.jar /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar
}

main "$@"
