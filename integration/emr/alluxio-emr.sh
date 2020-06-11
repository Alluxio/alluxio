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
readonly ALLUXIO_VERSION="2.3.0-SNAPSHOT"
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
    # TODO Add metadata header tag to the wget for filtering out in download metrics.
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

# Appends a property KV pair to the alluxio-site.properties file
# 
# Args:
#   $1: property
#   $2: value
append_alluxio_property() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function append_alluxio_property, expecting 2"
    exit 2
  fi
  local property="$1"
  local value="$2"
  
  if grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null; then
    echo "Property ${property} already exists in ${ALLUXIO_SITE_PROPERTIES}" >&2
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
  
  download_file "${alluxio_tarball}"

  release=$(basename "${alluxio_tarball}")
  local release_unzip
  if [[ "${release}" == *-* ]]; then
    release_unzip="${release%%-*}" # trims everything after the first '-', ex. alluxio-foo-bar-whatever -> alluxio
  else
    release_unzip="${release%%.tar*}" # trims everything after the '.tar', ex. alluxio.tar.gz -> alluxio
  fi
  # Unpack and inflate the release tar
  # TODO logic for different compression formats, s3 URIs, git URIs, etc.
  sudo cp "${release}" /opt/
  sudo tar -xpvf "/opt/${release}" -C /opt/
  sudo rm -R "/opt/${release}"
  sudo mv "/opt/${release_unzip}"* "${ALLUXIO_HOME}"
  sudo chown -R alluxio:alluxio "${ALLUXIO_HOME}"
  rm "${release}"

  # Add ${ALLUXIO_HOME}/bin to PATH for all users
  echo "export PATH=$PATH:${ALLUXIO_HOME}/bin" | sudo tee /etc/profile.d/alluxio.sh
}

#################
# Main function #
#################
main() {

  print_help() {
    local -r USAGE=$(cat <<USAGE_END

Usage: alluxio-emr.sh <root-ufs-uri>
                      [-b <backup_uri>]
                      [-c]
                      [-d <alluxio-download-uri>]
                      [-f <file_uri>]
                      [-i <journal_backup_uri>]
                      [-n <storage percentage>]
                      [-p <delimited_properties>]
                      [-s <property_delimiter>]

alluxio-emr.sh is a script which can be used to bootstrap an AWS EMR cluster
with Alluxio. It can download and install Alluxio as well as add properties
specified as arguments to the script.

By default, if the environment this script executes in does not already contain
an Alluxio install at ${ALLUXIO_HOME} then it will download, untar, and configure
the environment at ${ALLUXIO_HOME}. If an install already exists at ${ALLUXIO_HOME},
nothing will be installed over it, even if -d is specified.

If a different Alluxio version is desired, see the -d option.

  <root-ufs-uri>    The URI of the root UFS in the Alluxio namespace. If this
                    is an empty string, the emr hdfs root will be used as the
                    root UFS.

  -b                An s3:// URI that the Alluxio master will write a backup
                    to upon shutdown of the EMR cluster. The backup and and
                    upload MUST be run within 60 seconds. If the backup cannot
                    finish within 60 seconds, then an incomplete journal may
                    be uploaded. This option is not recommended for production
                    or mission critical use cases where the backup is relied
                    upon to restore cluster state after a previous shutdown.

  -c                Install the alluxio client jars only

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
                    provided in the [-p] argument.
USAGE_END
)
    echo -e "${USAGE}" >&2
    exit 1
  }
  local alluxio_tarball=""
  local backup_uri=""
  local property_delimiter=";"
  local delimited_properties=""
  local restore_from_backup_uri=""
  local files_list=""
  local nvme_capacity_usage=""
  local client_only="false"

  if [[ "$#" -lt "1" ]]; then
    echo -e "No root UFS URI provided"
    print_help 1
  fi

  local root_ufs_uri="${1}"
  shift
  while getopts "b:cd:f:i:n:p:s:" option; do
    if [[ -n "${OPTARG-}" ]]; then
      OPTARG=$(echo -e "${OPTARG}" | tr -d '[:space:]')
    fi
    case "${option}" in
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
      p)
        delimited_properties="${OPTARG}"
        ;;
      s)
        property_delimiter="${OPTARG}"
        ;;
      n)
        nvme_capacity_usage="${OPTARG}"
        ;;
      *)
        print_help 1
        ;;
    esac
  done

  if [[ "${nvme_capacity_usage}" ]]; then
    if [[ "${nvme_capacity_usage}" -lt 1 || "${nvme_capacity_usage}" -gt 100 ]]; then
      echo "The percent usage of NVMe storage usage must be between 1 and 100"
      exit 1
    fi
  fi

  # Create user
  id -u alluxio &>/dev/null || sudo useradd alluxio

  # Install Alluxio
  if [[ -z "${alluxio_tarball}" ]]; then
    alluxio_tarball="${ALLUXIO_DOWNLOAD_URL}"
  fi
  if [[ ! -d "${ALLUXIO_HOME}" ]]; then
    emr_install_alluxio "${alluxio_tarball}"
  fi

  if [[ ! -d "${ALLUXIO_HOME}" ]]; then
    echo -e "${ALLUXIO_HOME} install not found. Please provide a download URI with -d or install it on the OS before running this script."
    echo -e "Example URI: https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz"
    exit 1
  fi

  local -r aws_region=$(get_aws_region)

  # Get hostnames and load into masters/workers file
  local -r emr_cluster=$(jq '.jobFlowId' /mnt/var/lib/info/job-flow.json | sed -e 's/^"//' -e 's/"$//')
  local -r hostlist=$(aws emr list-instances --cluster-id "${emr_cluster}" --region "${aws_region}" | jq '.Instances[].PrivateDnsName' | sed -e 's/^"//' -e 's/"$//')

  # Should succeed only on workers. Otherwise, var is left empty
  local master
  master=$(jq '.masterHost' /mnt/var/lib/info/extraInstanceData.json | sed -e 's/^"//' -e 's/"$//' | nslookup | awk '/name/{print substr($NF,1,length($NF)-1)}')

  # Logic to get master hostname if on the master
  if [[ -z "${master}" ]]; then
    master=$(hostname)
    if [[ ${aws_region} == "us-east-1" ]]; then
      master=${master}".ec2.internal"
    else
      master=${master}".${aws_region}.compute.internal"
    fi
  fi

  local -r workers=$(printf '%s\n' "${hostlist//$master/}")

  doas alluxio "echo '${master}' > ${ALLUXIO_HOME}/conf/masters"
  doas alluxio "echo '${workers}' > ${ALLUXIO_HOME}/conf/workers"

  # set root ufs uri
  if [[ -z "${root_ufs_uri}" ]]; then
    root_ufs_uri="hdfs://${master}:8020"
  fi

  # Identify master
  local -r is_master=$(jq '.isMaster' /mnt/var/lib/info/instance.json)

  # Download files provided by "-f" to ${ALLUXIO_HOME}/conf
  IFS=" " read -ra files_to_be_downloaded <<< "${files_list}"
  if [ "${#files_to_be_downloaded[@]}" -gt "0" ]; then
    local filename
    for file in "${files_to_be_downloaded[@]}"; do
      filename="$(basename "${file}")"
      download_file "${file}"
      sudo mv "${filename}" "${ALLUXIO_HOME}/conf/${filename}"
    done
    sudo chown -R alluxio:alluxio "${ALLUXIO_HOME}/conf"
  fi
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

  local -r mem_size=$(get_default_mem_size)

  # Query S3 for canonical ID of user and strip out the quotes
  local -r canonical_id="$(aws s3api list-buckets --query "Owner.ID" | sed "s/\"//g")"


  # Append default configs to site properties if the user hasn't set them already
  doas alluxio "echo '# BEGIN AUTO-GENERATED PROPERTIES' >> ${ALLUXIO_SITE_PROPERTIES}"
  doas alluxio "echo '# Override these by specifying an alluxio-site.properties file, or delimited args within the EMR bootstrap' >> ${ALLUXIO_SITE_PROPERTIES}"

  local use_mem="true"

  # Configure NVMe storage if necessary
  if [[ "${nvme_capacity_usage}" ]]; then
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

      use_mem=""
      append_alluxio_property alluxio.worker.tieredstore.level0.alias "SSD"
      append_alluxio_property alluxio.worker.tieredstore.level0.dirs.mediumtype "${medium_type}"
      append_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "${paths}"
      append_alluxio_property alluxio.worker.tieredstore.level0.dirs.quota "${quotas}"
    fi

  fi

  if [[ "${use_mem}" ]]; then
    append_alluxio_property alluxio.worker.tieredstore.level0.dirs.quota "${mem_size}"
    append_alluxio_property alluxio.worker.tieredstore.level0.alias "MEM"
    append_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "/mnt/ramdisk"
  fi

  append_alluxio_property alluxio.user.file.writetype.default "ASYNC_THROUGH"
  append_alluxio_property alluxio.master.hostname "${master}"
  append_alluxio_property alluxio.master.journal.type "UFS"
  append_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"
  append_alluxio_property alluxio.master.security.impersonation.hive.users "*"
  append_alluxio_property alluxio.master.security.impersonation.presto.users "*"
  append_alluxio_property alluxio.master.security.impersonation.yarn.users "*"
  append_alluxio_property alluxio.worker.tieredstore.levels "1"
  append_alluxio_property alluxio.security.authorization.permission.enabled "false"
  append_alluxio_property alluxio.underfs.s3.owner.id.to.username.mapping "${canonical_id}=hadoop"
  doas alluxio "echo '# END AUTO-GENERATED PROPERTIES' >> ${ALLUXIO_SITE_PROPERTIES}"

  if [  "${client_only}" != "true" ]; then 
    # Alluxio can't rely on SSH to start services (i.e. no alluxio-start.sh all)
    if [ "${is_master}" = "true" ]; then
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
    else
      if [[ "${use_mem}" ]]; then
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
  # Compute application configs
  doas alluxio "ln -s ${ALLUXIO_HOME}/client/*client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar"
  sudo mkdir -p /usr/lib/spark/jars/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/spark/jars/alluxio-client.jar
  sudo mkdir -p /usr/lib/presto/plugin/hive-hadoop2/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/presto/plugin/hive-hadoop2/alluxio-client.jar

  # Create a symbolic link in presto plugin directory pointing to our connector if alluxio version is above 2.2
  for plugindir in "${ALLUXIO_HOME}"/client/presto/plugins/prestodb*; do
    # guard against using an older version by checking for alluxio connector's existence
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

  sudo mkdir -p /usr/lib/tez/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/tez/lib/alluxio-client.jar
  sudo mkdir -p /usr/lib/hadoop/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" /usr/lib/hadoop/lib/alluxio-client.jar
}

main "$@"
