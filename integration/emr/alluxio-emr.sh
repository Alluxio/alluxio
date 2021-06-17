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

set -eux

####################
# Global constants #
####################
readonly ALLUXIO_HOME="/opt/alluxio"
readonly ALLUXIO_SITE_PROPERTIES="${ALLUXIO_HOME}/conf/alluxio-site.properties"
readonly AWS_SHUTDOWN_ACTIONS_DIR="/mnt/var/lib/instance-controller/public/shutdown-actions"
readonly HADOOP_CONF="/etc/hadoop/conf"
readonly ALLUXIO_VERSION="2.7.0-SNAPSHOT"
readonly ALLUXIO_DOWNLOAD_URL="https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz"

####################
# Helper functions #
####################
# Downloads a file to the local machine into the cwd
# For the given scheme, uses the corresponding tool to download:
# s3://   -> aws s3 cp
# gs://   -> gsutil cp
# default -> wget
#
# Args:
#   $1: uri - S3, GS, or HTTP(S) URI to download from
download_file() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function download_file, expecting 1"
    exit 2
  fi
  local uri="$1"

  if [[ "${uri}" == s3://* ]]; then
    aws s3 cp "${uri}" ./
  elif [[ "${uri}" == gs://* ]]; then
    gsutil cp "${uri}" ./
  else
    wget -nv "${uri}"
  fi
}

# Run a command as a specific user
# Assumes the provided user already exists on the system and user running script has sudo access
#
# Args:
#   $1: user
#   $2: cmd
doas() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function doas, expecting 2"
    exit 2
  fi
  local user="$1"
  local cmd="$2"

  sudo runuser -l "${user}" -c "${cmd}"
}

# Appends or replaces a property KV pair to the alluxio-site.properties file
#
# Args:
#   $1: property
#   $2: value
set_alluxio_property() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function set_alluxio_property, expecting 2"
    exit 2
  fi
  local property="$1"
  local value="$2"

  if grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null; then
    doas alluxio "sed -i 's;${property}=.*;${property}=${value};g' ${ALLUXIO_SITE_PROPERTIES}"
    echo "Property ${property} already exists in ${ALLUXIO_SITE_PROPERTIES} and is replaced with value ${value}" >&2
  else
    doas alluxio "echo '${property}=${value}' >> ${ALLUXIO_SITE_PROPERTIES}"
  fi
}

# Calculates the default memory size as 1/3 of the total system memory
# Echo's the result to stdout. To store the return value in a variable use
# val=$(get_default_mem_size)
get_default_mem_size() {
  local -r mem_div=3
  phy_total=$(free -m | grep -oP '\d+' | head -n1)
  mem_size=$(( phy_total / mem_div ))
  echo "${mem_size}MB"
}

# Gets the region of the current EC2 instance
get_aws_region() {
  curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//'
}

# Puts a shutdown hook under the EMR defined /mnt/var/lib/instance-controller/public/shutdown-actions directory.
#
# Args:
#   $1: backup_uri - S3 URI to write backup file to
register_backup_on_shutdown() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function register_backup_on_shutdown, expecting 1"
    exit 2
  fi
  local backup_uri="$1"

  mkdir -p "${AWS_SHUTDOWN_ACTIONS_DIR}"
  BACKUP_DIR=/tmp/alluxio_backups
  echo "#!/usr/bin/env bash

  # This script will shut down, and then back up and upload the Alluxio journal to
  # the S3 path ${AWS_SHUTDOWN_ACTIONS_DIR}. The path can then be used in
  # conjunction with the -i (restore from backup) option.
  set -x

  mkdir -p ${BACKUP_DIR}
  chmod 777 ${BACKUP_DIR}
  ${ALLUXIO_HOME}/bin/alluxio fsadmin backup ${BACKUP_DIR} --local
  aws s3 cp --recursive ${BACKUP_DIR} \"${backup_uri}\"

  " > "${AWS_SHUTDOWN_ACTIONS_DIR}/alluxio-backup.sh"
}

# Installs alluxio to /opt/alluxio
#
# Args:
#   $1: alluxio_tarball - S3 or HTTP URI to Alluxio tarball
emr_install_alluxio() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function emr_install_alluxio, expecting 1"
    exit 2
  fi
  local alluxio_tarball="$1"
  echo "Downloading tarball"
  download_file "${alluxio_tarball}"
  echo "Tarball downloaded"
  release=$(basename "${alluxio_tarball}")
  echo "Tarball basename is ${release}"
  local release_unzip
  if [[ "${release}" == *-* ]]; then
    release_unzip="${release%%-*}" # trims everything after the first '-', ex. alluxio-foo-bar-whatever -> alluxio
  else
    release_unzip="${release%%.tar*}" # trims everything after the '.tar', ex. alluxio.tar.gz -> alluxio
  fi
  # Unpack and inflate the release tar
  # TODO logic for different compression formats, s3 URIs, git URIs, etc.
  echo "Copying tarball into /opt"
  sudo cp "${release}" /opt/
  sudo tar -xpvf "/opt/${release}" -C /opt/
  sudo rm -R "/opt/${release}"
  sudo mv "/opt/${release_unzip}"* "${ALLUXIO_HOME}"
  sudo chown -R alluxio:alluxio "${ALLUXIO_HOME}"
  rm "${release}"

  # Add ${ALLUXIO_HOME}/bin to PATH for all users
  echo "export PATH=$PATH:${ALLUXIO_HOME}/bin" | sudo tee /etc/profile.d/alluxio.sh
}

# Waits for the corresponding hadoop process to be running
#
# Args:
#   $1: is_master - "true" if instance is a master node, "false" if worker
wait_for_hadoop() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function wait_for_hadoop, expecting 1"
    exit 2
  fi
  local is_master="$1"

  local hadoop_process_name
  if [[ "${is_master}" == "true" ]]; then
    hadoop_process_name="NameNode"
  else
    hadoop_process_name="DataNode"
  fi
  hdfs_pid="-1"
  while ! sudo jps | grep "${hdfs_pid} ${hadoop_process_name}"; do
    sleep 5
    # each java process, grouped by user, stores their pids in /tmp/hsperfdata_<user>
    set +e
    pid=$(sudo ls /tmp/hsperfdata_hdfs)
    set -e
    # only set hdfs_pid if pid exists
    if [[ "${pid}" != "" ]]; then
      hdfs_pid="${pid}"
    fi
    echo "Found pid for ${hadoop_process_name}: ${hdfs_pid}"
  done
}

# Configures Alluxio to use NVMe mounts as storage
# Returns "true" if Alluxio should configure MEM tier when no NVMe mounts are available
#
# Args:
#   $1: nvme_capacity_usage - Argument value of [-n <storage percentage>]
configure_nvme() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function configure_nvme, expecting 1"
    exit 2
  fi
  nvme_capacity_usage=$1

  local use_mem="true"
  local paths=""
  local quotas=""
  local medium_type=""
  # Retrieve paths of NVMe devices who are mounted at /mnt*
  # in the format of "<dev name> <capacity> <mount path>"
  # The block size parameter (-B) is in MB (1024 * 1024)
  local -r mount_points="$(df -B 1048576 | grep 'nvme' | grep "/mnt" | awk '{print $1, $4, $6}')"
  set +e
  # read returns 1 unless EOF is reached, but we specify -d '' which means always read until EOF
  IFS=$'\n' read -d '' -ra mounts <<< "${mount_points}"
  set -e
  # attempt to configure NVMe, otherwise fallback to MEM
  if [[ "${#mounts[@]}" -gt 0 ]]; then
    for mount_point in "${mounts[@]}"; do
      local path_cap
      local mnt_path
      local quota_p
      path_cap="$(echo "${mount_point}" | awk '{print $2}')"
      mnt_path="$(echo "${mount_point}" | awk '{print $3}')"
      quota_p=$((path_cap * nvme_capacity_usage / 100))
      # if alluxio doesn't have permissions to write to this directory it will fail
      mnt_path+="/alluxio"
      sudo mkdir -p "${mnt_path}"
      sudo chown -R alluxio:alluxio "${mnt_path}"
      sudo chmod 777 "${mnt_path}"
      paths+="${mnt_path},"
      quotas+="${quota_p}MB,"
      medium_type+="SSD,"
    done
    paths="${paths::-1}"
    quotas="${quotas::-1}"
    medium_type="${medium_type::-1}"

    use_mem="false"
    set_alluxio_property alluxio.worker.tieredstore.level0.alias "SSD"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.mediumtype "${medium_type}"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "${paths}"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.quota "${quotas}"
  fi
  echo "${use_mem}"
}

####################
# Task functions #
####################
# Download remote user provided files into ${ALLUXIO_HOME}/conf folder
#
# Args:
#   $1: files list - Argument value of [-f <file_uri>]
download_user_files() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function download_user_files, expecting 1"
    exit 2
  fi
  files_list=$1
  IFS=" " read -ra files_to_be_downloaded <<< "${files_list}"
  if [ "${#files_to_be_downloaded[@]}" -gt "0" ]; then
    echo "Downloading files into ${ALLUXIO_HOME}/conf/}"
    local filename
    for file in "${files_to_be_downloaded[@]}"; do
      filename="$(basename "${file}")"
      download_file "${file}"
      sudo mv "${filename}" "${ALLUXIO_HOME}/conf/${filename}"
    done
    sudo chown -R alluxio:alluxio "${ALLUXIO_HOME}/conf"
  fi
}
# Expose alluxio client jar to compute applications
# This needs to happen before compute applications start so they can pick up the jar
expose_alluxio_client_jar() {
  doas alluxio "ln -s ${ALLUXIO_HOME}/client/*client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar"
  sudo mkdir -p /usr/lib/spark/jars/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/spark/jars/alluxio-client.jar
  sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar
  sudo mkdir -p /usr/lib/tez/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/tez/lib/alluxio-client.jar
  sudo mkdir -p /usr/lib/hadoop/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/hadoop/lib/alluxio-client.jar
}

# Set custom alluxio properties
#
# Args:
#   $1: delimited_properties - Argument value of [-p <delimited_properties>]
set_custom_alluxio_properties() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function set_custom_alluxio_properties, expecting 1"
    exit 2
  fi
  delimited_properties=$1
  # add newline to alluxio-site.properties in case the provided file doesn't end in newline
  doas alluxio "echo >> ${ALLUXIO_SITE_PROPERTIES}"
  if [[ "${delimited_properties}" ]]; then
    # inject user defined properties from args
    echo "Setting user defined properties in ${ALLUXIO_SITE_PROPERTIES}"
    IFS="${delimiter}" read -ra conf <<< "${delimited_properties}"
    for property in "${conf[@]}"; do
      local key=${property%%"="*}
      local value=${property#*"="}
      set_alluxio_property "${key}" "${value}"
    done
  fi
}

# Configure alluxio worker storage
#
# Args:
#   $1: nvme_capacity_usage - Argument value of [-n <storage percentage>]
configure_alluxio_worker_storage_properties() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function configure_alluxio_worker_storage_properties, expecting 1"
    exit 2
  fi
  nvme_capacity_usage=$1

  # configure NVMe storage if flag is set
  local use_mem="true"
  if [[ "${nvme_capacity_usage}" ]]; then
    use_mem=$(configure_nvme "${nvme_capacity_usage}")
  fi
  if [[ "${use_mem}" == "true" ]]; then
    local -r mem_size=$(get_default_mem_size)

    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.quota "${mem_size}"
    set_alluxio_property alluxio.worker.tieredstore.level0.alias "MEM"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "/mnt/ramdisk"
  fi
  echo "${use_mem}"
}

# Configure alluxio auto-generated default properties
#
# Args:
#   $1: master - alluxio master hostname
configure_alluxio_general_properties() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function configure_alluxio_general_properties, expecting 1"
    exit 2
  fi
  master=$1

  # query S3 for canonical ID of user and strip out the quotes
  local -r canonical_id="$(aws s3api list-buckets --query "Owner.ID" | sed "s/\"//g")"

  set_alluxio_property alluxio.user.file.writetype.default "ASYNC_THROUGH"
  set_alluxio_property alluxio.master.hostname "${master}"
  set_alluxio_property alluxio.master.journal.type "UFS"
  set_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"
  set_alluxio_property alluxio.master.security.impersonation.hive.users "*"
  set_alluxio_property alluxio.master.security.impersonation.presto.users "*"
  set_alluxio_property alluxio.master.security.impersonation.yarn.users "*"
  set_alluxio_property alluxio.worker.tieredstore.levels "1"
  set_alluxio_property alluxio.security.authorization.permission.enabled "false"
  set_alluxio_property alluxio.underfs.s3.owner.id.to.username.mapping "${canonical_id}=hadoop"
}

# Configure alluxio hdfs root mount if the given root is hdfs
#
# Args:
#   $1: root_ufs_uri - alluxio root ufs uri
#   $2: hdfs_version - if root ufs is hdfs, the version of this hdfs, can be empty
configure_alluxio_hdfs_root_mount() {
  if [[ "$#" -lt "1" ]]; then
    echo "Incorrect number of arguments passed into function configure_alluxio_general_properties, expecting at least 1"
    exit 2
  fi
  root_ufs_uri=$1
  if [[ "${root_ufs_uri}" = hdfs://* ]]; then
    if [[ "$#" -ne "2" ]]; then
      echo "Incorrect number of arguments passed into function configure_alluxio_worker_storage_properties, expecting 2"
      exit 2
    fi
    hdfs_version=$2
    set_alluxio_property alluxio.master.mount.table.root.option.alluxio.underfs.version "${hdfs_version}"
    # core-site.xml and hdfs-site.xml downloaded from the file list will override the default one
    core_site_location="${HADOOP_CONF}/core-site.xml"
    hdfs_site_location="${HADOOP_CONF}/hdfs-site.xml"
    if [[ -f "${ALLUXIO_HOME}/conf/core-site.xml" ]]; then
      core_site_location="${ALLUXIO_HOME}/conf/core-site.xml"
    fi
    if [[ -f "${ALLUXIO_HOME}/conf/hdfs-site.xml" ]]; then
      hdfs_site_location="${ALLUXIO_HOME}/conf/hdfs-site.xml"
    fi
    set_alluxio_property alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration "${core_site_location}:${hdfs_site_location}"
  fi
}

#################
# Main function #
#################
# The whole main function has the following execution sequence
# 1. Download and Install Alluxio tarball. Download remote files if any
# 2. Alluxio client jar is added to the classpath of applications
# 3. Waiting for hadoop process to come up
# 4. Configure and start Alluxio processes
main() {
    print_help() {
    local -r USAGE=$(cat <<USAGE_END

Usage: alluxio-emr.sh <root-ufs-uri>
                      [-b <backup_uri>]
                      [-c]
                      [-d <alluxio_download_uri>]
                      [-f <file_uri>]
                      [-i <journal_backup_uri>]
                      [-l <sync_list>]
                      [-n <storage percentage>]
                      [-p <delimited_properties>]
                      [-s <delimiter>]
                      [-v <hdfs_version>]

alluxio-emr.sh is a script which can be used to bootstrap an AWS EMR cluster
with Alluxio. It can download and install Alluxio as well as add properties
specified as arguments to the script.

By default, if the environment this script executes in does not already contain
an Alluxio install at ${ALLUXIO_HOME} then it will download, untar, and configure
the environment at ${ALLUXIO_HOME}. If an install already exists at ${ALLUXIO_HOME},
nothing will be installed over it, even if -d is specified.

If a different Alluxio version is desired, see the -d option.

  <root-ufs-uri>    (Required) The URI of the root UFS in the Alluxio namespace.
                    If this is the string "LOCAL", the EMR HDFS root will be used
                    as the root UFS.

  -b                An s3:// URI that the Alluxio master will write a backup
                    to upon shutdown of the EMR cluster. The backup and and
                    upload MUST be run within 60 seconds. If the backup cannot
                    finish within 60 seconds, then an incomplete journal may
                    be uploaded. This option is not recommended for production
                    or mission critical use cases where the backup is relied
                    upon to restore cluster state after a previous shutdown.

  -c                Install the alluxio client jars only.

  -d                An s3:// or http(s):// URI which points to an Alluxio
                    tarball. This script will download and untar the
                    Alluxio tarball and install Alluxio at ${ALLUXIO_HOME} if an
                    Alluxio installation doesn't already exist at that location.

  -f                An s3:// or http(s):// URI to any remote file. This property
                    can be specified multiple times. Any file specified through
                    this property will be downloaded and stored with the same
                    name to ${ALLUXIO_HOME}/conf/

  -i                An s3:// or http(s):// URI which represents the URI of a
                    previous Alluxio journal backup. If supplied, the backup
                    will be downloaded, and upon Alluxio startup, the Alluxio
                    master will read and restore the backup.

  -l                A string containing a delimited list of Alluxio paths.
                    Active sync will be enabled for the given paths. UFS
                    metadata will be periodically synced with the Alluxio
                    namespace. The delimiter by default is a semicolon ";". If a
                    different delimiter is desired use the [-s] argument.

  -n                Automatically configure NVMe storage for Alluxio workers at
                    tier 0 instead of MEM. When present, the script will attempt
                    to locate mounted NVMe storage locations and configure them
                    to be used with Alluxio. The argument provided is an
                    integer between 1 and 100 that represents the percentage of
                    each disk that will be allocated to Alluxio.

  -p                A string containing a delimited set of properties which
                    should be added to the
                    ${ALLUXIO_HOME}/conf/alluxio-site.properties file. The
                    delimiter by default is a semicolon ";". If a different
                    delimiter is desired use the [-s] argument.

  -s                A string containing a single character representing what
                    delimiter should be used to split the Alluxio properties
                    provided in the [-p] argument and the sync list provided
                    in the [-l] argument.

  -v                Version of HDFS used as the root UFS. Required when
                    root UFS is HDFS.

USAGE_END
)
    echo -e "${USAGE}" >&2
    exit 1
  }

  # ordered alphabetically by flag character
  local backup_uri=""
  local client_only="false"
  local alluxio_tarball=""
  local execute_synchronous="false"
  local files_list=""
  local restore_from_backup_uri=""
  local nvme_capacity_usage=""
  local delimited_properties=""
  local delimiter=";"
  local hdfs_version=""
  local sync_list=""

  if [[ "$#" -lt "1" ]]; then
    echo -e "No root UFS URI provided"
    print_help 1
  fi

  local root_ufs_uri="${1}"
  # note that since the script args are shifted
  # the shifted argument needs to be manually added when launching the background process
  shift

  while getopts "ehb:cd:f:i:l:n:p:s:v:" option; do
    OPTARG=$(echo -e "${OPTARG}" | tr -d '[:space:]')
    case "${option}" in
      e)
        # reserved flag for launching the script asynchronously
        execute_synchronous="true"
        ;;
      h)
        print_help 0
        ;;

      b)
        backup_uri="${OPTARG}"
        ;;
      c)
        client_only="true"
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
      l)
        sync_list="${OPTARG}"
        ;;
      n)
        nvme_capacity_usage="${OPTARG}"
        ;;
      p)
        delimited_properties="${OPTARG}"
        ;;
      s)
        delimiter="${OPTARG}"
        ;;
      v)
        hdfs_version="${OPTARG}"
        ;;
      *)
        print_help 1
        ;;
    esac
  done

  # validate arguments
  if [[ "${nvme_capacity_usage}" ]]; then
    if [[ "${nvme_capacity_usage}" -lt 1 || "${nvme_capacity_usage}" -gt 100 ]]; then
      echo "The percent usage of NVMe storage usage must be between 1 and 100"
      exit 1
    fi
  fi
  if [[ -z "${alluxio_tarball}" ]]; then
    alluxio_tarball="${ALLUXIO_DOWNLOAD_URL}"
  fi
  if [[ "${root_ufs_uri}" = hdfs://* ]] && [[ -z "${hdfs_version}" ]]; then
    echo "Hdfs version of Alluxio HDFS root mount must be provided with -v"
    exit 1
  fi

  # self-invoke script as background task
  # this allows EMR to continue installing and launching applications
  # the script will wait until HDFS processes are running before continuing
  if [[ ${execute_synchronous} == "false" ]]; then
    echo "Executing synchronously before hadoop starts"
    # create user, install Alluxio if not exists, create masters and workers files
    id -u alluxio &>/dev/null || sudo useradd alluxio
    echo "Created alluxio user"
    if [[ ! -d "${ALLUXIO_HOME}" ]]; then
      echo "Installing Alluxio from tarball at ${alluxio_tarball}"
      emr_install_alluxio "${alluxio_tarball}"
    fi
    if [[ ! -d "${ALLUXIO_HOME}" ]]; then
      echo -e "${ALLUXIO_HOME} install not found. Please provide a download URI with -d or install it on the OS before running this script."
      echo -e "Example URI: https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz"
      exit 1
    fi
    download_user_files "${files_list}"
    expose_alluxio_client_jar

    echo "Launching background process"
    # note the root_ufs_uri needs to be manually added
    # because shift removes it from the arguments array
    launch_args="$0 ${root_ufs_uri} -e"
    # iterate through each provided argument and replace characters that need escaping
    # this is necessary to properly wrap the json value of a flag
    # otherwise the json content will not be passed correctly into the background process
    input_args=( "$@" )
    if [[ "${#input_args[@]}" -gt "0" ]]; then
      for i in "${input_args[@]}"; do
        # handle quoted arguments: https://unix.stackexchange.com/questions/187651/how-to-echo-single-quote-when-using-single-quote-to-wrap-special-characters-in
        esc_i=$(sed "s/'"'/&\\&&/g
     s/.*/'"'&'"'/
' <<IN
$i
IN
)
        launch_args="${launch_args} ${esc_i}"
      done
    fi
    bash -c "$launch_args" &
    exit 0
  fi
  echo "Executing asynchronous"

  # collect instance information
  local -r local_hostname=$(hostname -f)
  local -r is_master=$(jq '.isMaster' /mnt/var/lib/info/instance.json)

  # determine master hostname, different if on master vs worker
  local master
  if [[ "${is_master}" == "true" ]]; then
    master="${local_hostname}"
  else
    master=$(jq '.masterHost' /mnt/var/lib/info/extraInstanceData.json | sed -e 's/^"//' -e 's/"$//' | nslookup | awk '/name/{print substr($NF,1,length($NF)-1)}')
  fi

  # set root ufs uri
  if [[ "${root_ufs_uri}" == "LOCAL" ]]; then
    root_ufs_uri="hdfs://${master}:8020/"
  fi

  # wait until hadoop process is running
  echo "Waiting for processes to start before starting script"
  wait_for_hadoop "${is_master}"

  echo "Starting Alluxio configuration"
  # set auto generated properties
  echo "Setting auto-generated properties in ${ALLUXIO_SITE_PROPERTIES}"
  doas alluxio "echo '# BEGIN AUTO-GENERATED PROPERTIES' >> ${ALLUXIO_SITE_PROPERTIES}"

  local -r use_mem=$(configure_alluxio_worker_storage_properties "${nvme_capacity_usage}")
  configure_alluxio_general_properties "${master}"
  configure_alluxio_hdfs_root_mount "${root_ufs_uri}" "${hdfs_version}"

  doas alluxio "echo '# END AUTO-GENERATED PROPERTIES' >> ${ALLUXIO_SITE_PROPERTIES}"

  # set user provided properties
  set_custom_alluxio_properties "${delimited_properties}"

  # Create a symbolic link in presto plugin directory pointing to our connector if:
  # - emr version is 5.28  or 5.29 -> prestodb = 0.227 - 0.231
  # - alluxio version is above 2.2
  local -r emr_version=$(jq ".releaseLabel" /mnt/var/lib/info/extraInstanceData.json  | sed -e 's/^"emr-//' -e 's/"$//')
  local -r emr_major=$(echo "${emr_version}" | sed -s 's/\([[:digit:]]\+\)\.\([[:digit:]]\+\)\.[[:digit:]]\+/\1/')
  local -r emr_minor=$(echo "${emr_version}" | sed -s 's/\([[:digit:]]\+\)\.\([[:digit:]]\+\)\.[[:digit:]]\+/\2/')
  if [[ "${emr_major}" -eq 5 && "${emr_minor}" -ge 28 && "${emr_minor}" -lt 30  ]]; then
    for plugindir in "${ALLUXIO_HOME}"/client/presto/plugins/prestodb*; do
      # guard against using an older version by checking for alluxio connector's existence
      # use alluxio's bundled connector hive-alluxio
      if [ -d "$plugindir" ]; then
        doas alluxio "ln -s $plugindir ${ALLUXIO_HOME}/client/presto/plugins/prestodb_connector"
        sudo ln -s "${ALLUXIO_HOME}/client/presto/plugins/prestodb_connector" /usr/lib/presto/plugin/hive-alluxio
        sudo mkdir -p /etc/presto/conf/catalog
        echo "connector.name=hive-alluxio" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        echo "hive.metastore=alluxio" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        echo "hive.metastore.alluxio.master.address=${master}:19998" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        break
      fi
    done
  fi
  # Use prestodb's builtin connect with alluxio catalog service support if
  # - emr version is >= 5.30 -> prestodb >= 0.232
  # - alluxio version is above 2.2
  if [ "${emr_major}" -ge 6 ] ||  [[ "${emr_major}" -eq 5 && "${emr_minor}" -ge 30 ]]; then
    for plugindir in "${ALLUXIO_HOME}"/client/presto/plugins/prestodb*; do
      # guard against using an older version by checking for alluxio connector's existence
      # use prestodb's built-in connector hive-hadoop2
      if [ -d "$plugindir" ]; then
        sudo mkdir -p /etc/presto/conf/catalog
        echo "connector.name=hive-hadoop2" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        echo "hive.metastore=alluxio" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        echo "hive.metastore.alluxio.master.address=${master}:19998" | sudo tee -a /etc/presto/conf/catalog/catalog_alluxio.properties
        break
      fi
    done
  fi

  # start Alluxio cluster
  if [[ "${client_only}" != "true" ]]; then
    echo "Starting Alluxio cluster"
    if [[ ${is_master} = "true" ]]; then
      local args=""
      if [[ "${restore_from_backup_uri}" ]]; then
        local -r backup_name="$(basename "${restore_from_backup_uri}")"
        local -r backup_location=/tmp/alluxio_backup
        mkdir -p "${backup_location}"
        cd "${backup_location}"
        download_file "${restore_from_backup_uri}"
        chmod -R 777 "${backup_location}"
        args="-i ${backup_location}/${backup_name}"
      fi
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a ${args} master"
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a job_master"
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a proxy"
      if [[ "${backup_uri}" ]]; then
        register_backup_on_shutdown "${backup_uri}"
      fi
      until ${ALLUXIO_HOME}/bin/alluxio fsadmin report
      do
        sleep 5
      done
      if [[ "${sync_list}" ]]; then
        IFS="${delimiter}" read -ra paths <<< "${sync_list}"
        for path in "${paths[@]}"; do
          ${ALLUXIO_HOME}/bin/alluxio fs startSync "${path}"
        done
      fi
    else
      if [[ "${use_mem}" == "true" ]]; then
        ${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount local
      fi
      until ${ALLUXIO_HOME}/bin/alluxio fsadmin report
      do
        sleep 5
      done
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a worker"
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a job_worker"
      doas alluxio "${ALLUXIO_HOME}/bin/alluxio-start.sh -a proxy"
    fi
  fi

  echo "Alluxio bootstrap complete!"
}

main "$@"

