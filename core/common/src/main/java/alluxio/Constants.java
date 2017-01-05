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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * System wide constants.
 */
@ThreadSafe
public final class Constants {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final int GB = MB * 1024;
  public static final long TB = GB * 1024L;
  public static final long PB = TB * 1024L;

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  public static final String LS_FORMAT_PERMISSION = "%-15s";
  public static final String LS_FORMAT_FILE_SIZE = "%-10s";
  public static final String LS_FORMAT_CREATE_TIME = "%-25s";
  public static final String LS_FORMAT_FILE_TYPE = "%-15s";
  public static final String LS_FORMAT_USER_NAME = "%-15s";
  public static final String LS_FORMAT_GROUP_NAME = "%-15s";
  public static final String LS_FORMAT_FILE_PATH = "%-5s";
  public static final String LS_FORMAT = LS_FORMAT_PERMISSION + LS_FORMAT_USER_NAME
      + LS_FORMAT_GROUP_NAME + LS_FORMAT_FILE_SIZE + LS_FORMAT_CREATE_TIME + LS_FORMAT_FILE_TYPE
      + LS_FORMAT_FILE_PATH + "%n";
  public static final String LS_FORMAT_NO_ACL = LS_FORMAT_FILE_SIZE + LS_FORMAT_CREATE_TIME
      + LS_FORMAT_FILE_TYPE + LS_FORMAT_FILE_PATH + "%n";

  public static final String MESOS_RESOURCE_CPUS = "cpus";
  public static final String MESOS_RESOURCE_MEM = "mem";
  public static final String MESOS_RESOURCE_DISK = "disk";
  public static final String MESOS_RESOURCE_PORTS = "ports";

  public static final long SECOND_NANO = 1000000000L;
  public static final int SECOND_MS = 1000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;

  public static final int BYTES_IN_INTEGER = 4;

  public static final long UNKNOWN_SIZE = -1;

  public static final String SCHEME = "alluxio";
  public static final String HEADER = SCHEME + "://";

  public static final String SCHEME_FT = "alluxio-ft";
  public static final String HEADER_FT = SCHEME_FT + "://";

  public static final String HEADER_OSS = "oss://";

  public static final String HEADER_S3 = "s3://";
  public static final String HEADER_S3N = "s3n://";
  public static final String HEADER_S3A = "s3a://";
  public static final String HEADER_SWIFT = "swift://";
  // Google Cloud Storage header convention is "gs://".
  // See https://cloud.google.com/storage/docs/cloud-console
  public static final String HEADER_GCS = "gs://";

  public static final int MAX_PORT = 65535;

  // Service versions should be incremented every time a backwards incompatible change occurs.
  public static final long BLOCK_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long BLOCK_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final long BLOCK_WORKER_CLIENT_SERVICE_VERSION = 1;
  public static final long FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final long FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION = 1;
  public static final long LINEAGE_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long LINEAGE_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final long META_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long KEY_VALUE_WORKER_SERVICE_VERSION = 1;
  public static final long UNKNOWN_SERVICE_VERSION = -1;

  public static final String BLOCK_MASTER_NAME = "BlockMaster";
  public static final String FILE_SYSTEM_MASTER_NAME = "FileSystemMaster";
  public static final String LINEAGE_MASTER_NAME = "LineageMaster";
  public static final String KEY_VALUE_MASTER_NAME = "KeyValueMaster";

  public static final String BLOCK_MASTER_CLIENT_SERVICE_NAME = "BlockMasterClient";
  public static final String BLOCK_MASTER_WORKER_SERVICE_NAME = "BlockMasterWorker";
  public static final String FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME = "FileSystemMasterClient";
  public static final String FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME = "FileSystemMasterWorker";
  public static final String LINEAGE_MASTER_CLIENT_SERVICE_NAME = "LineageMasterClient";
  public static final String LINEAGE_MASTER_WORKER_SERVICE_NAME = "LineageMasterWorker";
  public static final String META_MASTER_SERVICE_NAME = "MetaMaster";
  public static final String BLOCK_WORKER_CLIENT_SERVICE_NAME = "BlockWorkerClient";
  public static final String FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME = "FileSystemWorkerClient";
  public static final String KEY_VALUE_MASTER_CLIENT_SERVICE_NAME = "KeyValueMasterClient";
  public static final String KEY_VALUE_WORKER_CLIENT_SERVICE_NAME = "KeyValueWorkerClient";

  public static final String REST_API_PREFIX = "/api/v1";

  public static final String LOGGER_TYPE = PropertyKey.Name.LOGGER_TYPE;

  public static final String MASTER_COLUMN_FILE_PREFIX = "COL_";
  public static final String FORMAT_FILE_PREFIX = "_format_";

  public static final long CLIENT_METRICS_VERSION = 1L;
  public static final int CLIENT_METRICS_SIZE = 13;

  public static final String SWIFT_AUTH_KEYSTONE = "keystone";
  public static final String SWIFT_AUTH_SWIFTAUTH = "swiftauth";

  public static final String MESOS_LOCAL_INSTALL = "LOCAL";

  /**
   * Maximum number of seconds to wait for thrift servers to stop on shutdown. Tests use a value of
   * 0 instead of this value so that they can run faster.
   */
  public static final int THRIFT_STOP_TIMEOUT_SECONDS = 60;

  // Time-to-live
  public static final long NO_TTL = -1;

  // Security
  public static final int DEFAULT_FILE_SYSTEM_UMASK = 0022;
  public static final short DEFAULT_FILE_SYSTEM_MODE = (short) 0777;
  public static final short FILE_DIR_PERMISSION_DIFF = (short) 0111;
  public static final short INVALID_MODE = -1;

  // Specific tier write
  public static final int FIRST_TIER = 0;
  public static final int SECOND_TIER = 1;
  public static final int LAST_TIER = -1;

  private Constants() {} // prevent instantiation
}
