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

  public static final long SECOND = 1000;
  public static final long MINUTE = SECOND * 60L;
  public static final long HOUR = MINUTE * 60L;
  public static final long DAY = HOUR * 24L;

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  public static final String EXTENSION_JAR = ".jar";

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
  public static final String HEADER_S3A = "s3a://";
  public static final String HEADER_SWIFT = "swift://";
  // Google Cloud Storage header convention is "gs://".
  // See https://cloud.google.com/storage/docs/cloud-console
  public static final String HEADER_GCS = "gs://";

  public static final int MAX_PORT = 65535;

  public static final int MAX_TEST_PROCESS_LIFETIME_MS = 20 * Constants.MINUTE_MS;

  // Service versions should be incremented every time a backwards incompatible change occurs.
  public static final long BLOCK_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long BLOCK_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long BLOCK_WORKER_CLIENT_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION = 2;
  public static final long LINEAGE_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long META_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long KEY_VALUE_WORKER_SERVICE_VERSION = 2;
  public static final long UNKNOWN_SERVICE_VERSION = -1;

  public static final String BLOCK_MASTER_NAME = "BlockMaster";
  public static final String FILE_SYSTEM_MASTER_NAME = "FileSystemMaster";
  public static final String LINEAGE_MASTER_NAME = "LineageMaster";
  public static final String KEY_VALUE_MASTER_NAME = "KeyValueMaster";
  public static final String BLOCK_WORKER_NAME = "BlockWorker";
  public static final String FILE_SYSTEM_WORKER_NAME = "FileSystemWorker";
  public static final String KEY_VALUE_WORKER_NAME = "KeyValueWorker";

  public static final String BLOCK_MASTER_CLIENT_SERVICE_NAME = "BlockMasterClient";
  public static final String BLOCK_MASTER_WORKER_SERVICE_NAME = "BlockMasterWorker";
  public static final String FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME = "FileSystemMasterClient";
  public static final String FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME = "FileSystemMasterWorker";
  public static final String LINEAGE_MASTER_CLIENT_SERVICE_NAME = "LineageMasterClient";
  public static final String META_MASTER_SERVICE_NAME = "MetaMaster";
  public static final String BLOCK_WORKER_CLIENT_SERVICE_NAME = "BlockWorkerClient";
  public static final String FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME = "FileSystemWorkerClient";
  public static final String KEY_VALUE_MASTER_CLIENT_SERVICE_NAME = "KeyValueMasterClient";
  public static final String KEY_VALUE_WORKER_CLIENT_SERVICE_NAME = "KeyValueWorkerClient";
  public static final String UFS_INPUT_STREAM_CACHE_EXPIRATION = "UfsInputStreamCacheExpiration";

  public static final int DEFAULT_REGISTRY_GET_TIMEOUT_MS = 60 * SECOND_MS;

  // Test-related constants
  public static final int MAX_TEST_DURATION_MS = 10 * MINUTE_MS;
  public static final String TEST_ARTIFACTS_DIR = "./target/artifacts";
  public static final String TEST_LOG_DIR = "./target/logs";
  public static final String TESTS_LOG = "./target/logs/tests.log";

  public static final String REST_API_PREFIX = "/api/v1";

  public static final String MASTER_COLUMN_FILE_PREFIX = "COL_";

  public static final String SITE_PROPERTIES = "alluxio-site.properties";

  public static final String SWIFT_AUTH_KEYSTONE = "keystone";
  public static final String SWIFT_AUTH_KEYSTONE_V3 = "keystonev3";
  public static final String SWIFT_AUTH_SWIFTAUTH = "swiftauth";

  public static final String LOCALITY_NODE = "node";
  public static final String LOCALITY_RACK = "rack";
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
  public static final String IMPERSONATION_HDFS_USER = "_HDFS_USER_";

  // Specific tier write
  public static final int FIRST_TIER = 0;
  public static final int SECOND_TIER = 1;
  public static final int LAST_TIER = -1;

  // S3 northbound API constants
  public static final String S3_DELETE_IN_ALLUXIO_ONLY = "ALLUXIO_ONLY";
  public static final String S3_DELETE_IN_ALLUXIO_AND_UFS = "ALLUXIO_AND_UFS";
  public static final String S3_MULTIPART_TEMPORARY_DIR_SUFFIX = "_s3_multipart_tmp";

  // Ufs fingerprint
  public static final String INVALID_UFS_FINGERPRINT = "";

  private Constants() {} // prevent instantiation
}
