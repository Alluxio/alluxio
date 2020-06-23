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
readonly MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly ALLUXIO_DOWNLOAD_PATH="$(/usr/share/google/get_metadata_value attributes/alluxio_download_path || true)"
readonly ALLUXIO_LICENSE_BASE64="$(/usr/share/google/get_metadata_value attributes/alluxio_license_base64 || true)"
readonly SPARK_HOME="${SPARK_HOME:-"/usr/lib/spark"}"
readonly HIVE_HOME="${HIVE_HOME:-"/usr/lib/hive"}"
readonly HADOOP_HOME="${HADOOP_HOME:-"/usr/lib/hadoop"}"
readonly PRESTO_HOME="$(/usr/share/google/get_metadata_value attributes/alluxio_presto_home || echo "/usr/lib/presto")"
readonly ALLUXIO_VERSION="2.4.0-SNAPSHOT"
readonly ALLUXIO_DOWNLOAD_URL="https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz"
readonly ALLUXIO_HOME="/opt/alluxio"
readonly ALLUXIO_SITE_PROPERTIES="${ALLUXIO_HOME}/conf/alluxio-site.properties"

####################
# Helper functions #
####################
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
    echo "${property}=${value}" >> ${ALLUXIO_SITE_PROPERTIES}
  fi
}

# Gets a value from a KV pair in the alluxio-site.properties file
# 
# Args:
#   $1: property
get_alluxio_property() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function get_alluxio_property, expecting 1"
    exit 2
  fi
  local property="$1"
  
  grep -e "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} | cut -d "=" -f2
}

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

# Calculates the default memory size as 1/3 of the total system memory
# Echo's the result to stdout. To store the return value in a variable use
# val=$(get_default_mem_size)
get_default_mem_size() {
  local -r mem_div=3
  phy_total=$(free -m | grep -oP '\d+' | head -n1)
  mem_size=$(( phy_total / mem_div ))
  echo "${mem_size}MB"
}

# Start the Alluxio server process
start_alluxio() {
  if [[ "${ROLE}" == "Master" ]]; then
    ${ALLUXIO_HOME}/bin/alluxio formatMaster
    systemctl restart alluxio-master alluxio-job-master

    local -r sync_list=$(/usr/share/google/get_metadata_value attributes/alluxio_sync_list || true)
    local path_delimiter=";"
    if [[ "${sync_list}" ]]; then
      IFS="${path_delimiter}" read -ra paths <<< "${sync_list}"
      for path in "${paths[@]}"; do
        ${ALLUXIO_HOME}/bin/alluxio fs startSync ${path}
      done
    fi
  else
    if [[ $(get_alluxio_property alluxio.worker.tieredstore.level0.alias) == "MEM" ]]; then
      ${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount local
    fi
    ${ALLUXIO_HOME}/bin/alluxio formatWorker
    systemctl restart alluxio-worker alluxio-job-worker
  fi
}

# Download the Alluxio tarball and untar to ALLUXIO_HOME
bootstrap_alluxio() {
  # Download the Alluxio tarball
  mkdir ${ALLUXIO_HOME}
  local download_url="${ALLUXIO_DOWNLOAD_URL}"
  if [ -n "${ALLUXIO_DOWNLOAD_PATH}" ]; then
    download_url=${ALLUXIO_DOWNLOAD_PATH}
  fi
  download_file "${download_url}"
  local tarball_name=${download_url##*/}
  tar -zxf "${tarball_name}" -C ${ALLUXIO_HOME} --strip-components 1
  ln -s ${ALLUXIO_HOME}/client/alluxio-${ALLUXIO_VERSION}-client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar

  # Download files to /opt/alluxio/conf
  local -r download_files_list=$(/usr/share/google/get_metadata_value attributes/alluxio_download_files_list || true)
  local download_delimiter=";"
  IFS="${download_delimiter}" read -ra files_to_be_downloaded <<< "${download_files_list}"
  if [ "${#files_to_be_downloaded[@]}" -gt "0" ]; then
    local filename
    for file in "${files_to_be_downloaded[@]}"; do
      filename="$(basename "${file}")"
      download_file "${file}"
      mv "${filename}" "${ALLUXIO_HOME}/conf/${filename}"
    done
  fi

  # Configure systemd services
  if [[ "${ROLE}" == "Master" ]]; then
    # The master role runs 2 daemons: AlluxioMaster and AlluxioJobMaster
    # Service for AlluxioMaster JVM
    cat >"/etc/systemd/system/alluxio-master.service" <<- EOF
[Unit]
Description=Alluxio Master
After=default.target
[Service]
Type=simple
User=root
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process master -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
    systemctl enable alluxio-master
    # Service for AlluxioJobMaster JVM
    cat >"/etc/systemd/system/alluxio-job-master.service" <<- EOF
[Unit]
Description=Alluxio Job Master
After=default.target
[Service]
Type=simple
User=root
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process job_master -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
    systemctl enable alluxio-job-master
  else
    # The worker role runs 2 daemons: AlluxioWorker and AlluxioJobWorker
    # Service for AlluxioWorker JVM
    cat >"/etc/systemd/system/alluxio-worker.service" <<- EOF
[Unit]
Description=Alluxio Worker
After=default.target
[Service]
Type=simple
User=root
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process worker -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
    systemctl enable alluxio-worker
    # Service for AlluxioJobWorker JVM
    cat >"/etc/systemd/system/alluxio-job-worker.service" <<- EOF
[Unit]
Description=Alluxio Job Worker
After=default.target
[Service]
Type=simple
User=root
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process job_worker -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
    systemctl enable alluxio-job-worker
  fi
  # Configure client applications
  mkdir -p "${SPARK_HOME}/jars/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${SPARK_HOME}/jars/alluxio-client.jar"
  mkdir -p "${HIVE_HOME}/lib/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${HIVE_HOME}/lib/alluxio-client.jar"
  mkdir -p "${HADOOP_HOME}/lib/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${HADOOP_HOME}/lib/alluxio-client.jar"
  if [[ "${ROLE}" == "Master" ]]; then
    systemctl restart hive-metastore hive-server2
  fi
  mkdir -p "${PRESTO_HOME}/plugin/hive-hadoop2/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${PRESTO_HOME}/plugin/hive-hadoop2/alluxio-client.jar"
  systemctl restart presto || echo "Presto service cannot be restarted"

  # Add ${ALLUXIO_HOME}/bin to PATH for all users
  echo "export PATH=$PATH:${ALLUXIO_HOME}/bin" | sudo tee /etc/profile.d/alluxio.sh

  # Optionally configure license
  if [ -n "${ALLUXIO_LICENSE_BASE64}" ]; then
    echo "${ALLUXIO_LICENSE_BASE64}" | base64 -d > ${ALLUXIO_HOME}/license.json
  fi
}

# Configure SSD if necessary and relevant alluxio-site.properties
configure_alluxio_storage() {
  local -r mem_size=$(get_default_mem_size)
  local -r owner="root"
  local -r ssd_capacity_usage=$(/usr/share/google/get_metadata_value attributes/alluxio_ssd_capacity_usage || true)
  local use_mem="true"

  if [[ "${ssd_capacity_usage}" ]]; then
    if [[ "${ssd_capacity_usage}" -lt 1 || "${ssd_capacity_usage}" -gt 100 ]]; then
      echo "The percent usage of ssd storage usage must be between 1 and 100"
      exit 1
    fi

    local paths=""
    local quotas=""
    local medium_type=""
    # Retrieve paths of ssd devices who are mounted at /mnt*
    # in the format of "<dev name> <capacity> <mount path>"
    # The block size parameter (-B) is in MB (1024 * 1024)
    local -r mount_points="$(lsblk -d -o name,rota | awk 'NR>1' |
      while read -r ROW;
      do
        dd=$(echo "$ROW" | awk '{print $2}');
        if [ "${dd}" -eq 0 ]; then
          df -B 1048576 | grep "$(echo "$ROW" | awk '{print $1}')" | grep "/mnt" | awk '{print $1, $4, $6}';
        fi;
      done
    )"
    set +e
    # read returns 1 unless EOF is reached, but we specify -d '' which means always read until EOF
    IFS=$'\n' read -d '' -ra mounts <<< "${mount_points}"
    set -e
    # attempt to configure ssd, otherwise fallback to MEM
    if [[ "${#mounts[@]}" -gt 0 ]]; then
      for mount_point in "${mounts[@]}"; do
    	  local path_cap
    	  local mnt_path
    	  local quota_p
    	  path_cap="$(echo "${mount_point}" | awk '{print $2}')"
    	  mnt_path="$(echo "${mount_point}" | awk '{print $3}')"
    	  quota_p=$((path_cap * ssd_capacity_usage / 100))
    	  # if alluxio doesn't have permissions to write to this directory it will fail
    	  mnt_path+="/alluxio"
    	  mkdir -p "${mnt_path}"
    	  chown -R ${owner} "${mnt_path}"
    	  chmod 777 "${mnt_path}"
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
    append_alluxio_property alluxio.worker.memory.size "${mem_size}"
    append_alluxio_property alluxio.worker.tieredstore.level0.alias "MEM"
    append_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "/mnt/ramdisk"
  fi
}

# Configure alluxio-site.properties
configure_alluxio() {
  cp ${ALLUXIO_HOME}/conf/alluxio-site.properties.template ${ALLUXIO_SITE_PROPERTIES}
  append_alluxio_property alluxio.master.hostname "${MASTER_FQDN}"
  append_alluxio_property alluxio.master.journal.type "UFS"
  local -r root_ufs_uri=$(/usr/share/google/get_metadata_value attributes/alluxio_root_ufs_uri)
  append_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"
  configure_alluxio_storage
  append_alluxio_property alluxio.worker.tieredstore.levels "1"
  append_alluxio_property alluxio.master.security.impersonation.root.users "*"
  append_alluxio_property alluxio.master.security.impersonation.root.groups "*"
  append_alluxio_property alluxio.master.security.impersonation.client.users "*"
  append_alluxio_property alluxio.master.security.impersonation.client.groups "*"
  append_alluxio_property alluxio.security.login.impersonation.username "_NONE_"
  append_alluxio_property alluxio.security.authorization.permission.enabled "true"
  append_alluxio_property alluxio.user.rpc.retry.max.duration "10min"
  local -r site_properties=$(/usr/share/google/get_metadata_value attributes/alluxio_site_properties || true)
  local property_delimiter=";"
  if [[ "${site_properties}" ]]; then
    IFS="${property_delimiter}" read -ra conf <<< "${site_properties}"
    for property in "${conf[@]}"; do
      local key=${property%%"="*}
      local value=${property#*"="}
      append_alluxio_property "${key}" "${value}"
    done
  fi
}

#################
# Main function #
#################
main() {
  echo "Alluxio version: ${ALLUXIO_VERSION}"
  bootstrap_alluxio
  configure_alluxio
  start_alluxio
}

main "$@"
