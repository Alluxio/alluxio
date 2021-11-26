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

  public static final int MS_NANO = 1_000_000;
  public static final long SECOND_NANO = 1_000_000_000L;
  public static final int SECOND_MS = 1_000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;
  public static final int MINUTE_SECONDS = 60;

  public static final int BYTES_IN_INTEGER = 4;

  public static final long UNKNOWN_SIZE = -1;

  public static final String NO_SCHEME = "alluxio-noop";
  public static final String SCHEME = "alluxio";
  public static final String HEADER = SCHEME + "://";

  // Under Filesystem URI Schemes
  public static final String HEADER_ABFS = "abfs://";
  public static final String HEADER_ABFSS = "abfss://";
  public static final String HEADER_ADL = "adl://";
  public static final String HEADER_ADLS = "adls://";
  public static final String HEADER_CEPHFS = "cephfs://";
  public static final String HEADER_CEPHFS_HADOOP = "ceph://";
  public static final String HEADER_COS = "cos://";
  public static final String HEADER_COSN = "cosn://";
  // Google Cloud Storage header convention is "gs://".
  // See https://cloud.google.com/storage/docs/cloud-console
  public static final String HEADER_GCS = "gs://";
  public static final String HEADER_HTTP = "http://";
  public static final String HEADER_HTTPS = "https://";
  public static final String HEADER_KODO = "kodo://";
  public static final String HEADER_OSS = "oss://";
  public static final String HEADER_OZONE = "o3fs://";
  public static final String HEADER_S3 = "s3://";
  public static final String HEADER_S3A = "s3a://";
  public static final String HEADER_SWIFT = "swift://";
  public static final String HEADER_WASB = "wasb://";
  public static final String HEADER_WASBS = "wasbs://";
  public static final String HEADER_OBS = "obs://";

  public static final int MAX_PORT = 65535;

  public static final int MAX_TEST_PROCESS_LIFETIME_MS = 20 * Constants.MINUTE_MS;

  // Service versions should be incremented every time a backwards incompatible change occurs.

  public static final long FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long BLOCK_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long BLOCK_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long META_MASTER_CONFIG_SERVICE_VERSION = 2;
  public static final long META_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long META_MASTER_MASTER_SERVICE_VERSION = 1;
  public static final long METRICS_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long JOURNAL_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long RAFT_JOURNAL_SERVICE_VERSION = 1;
  public static final long UNKNOWN_SERVICE_VERSION = -1;

  public static final String BLOCK_MASTER_NAME = "BlockMaster";
  public static final String FILE_SYSTEM_MASTER_NAME = "FileSystemMaster";
  public static final String META_MASTER_NAME = "MetaMaster";
  public static final String METRICS_MASTER_NAME = "MetricsMaster";
  public static final String BLOCK_WORKER_NAME = "BlockWorker";
  public static final String FILE_SYSTEM_WORKER_NAME = "FileSystemWorker";

  public static final String BLOCK_MASTER_CLIENT_SERVICE_NAME = "BlockMasterClient";
  public static final String BLOCK_MASTER_WORKER_SERVICE_NAME = "BlockMasterWorker";
  public static final String FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME = "FileSystemMasterClient";
  public static final String FILE_SYSTEM_MASTER_JOB_SERVICE_NAME = "FileSystemMasterJob";
  public static final String FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME = "FileSystemMasterWorker";
  // TODO(binfan): set META_MASTER_CLIENT_SERVICE_NAME to "MetaMasterClient" after 2.0.
  // Its value is "MetaMaster" for backwards compatibility so 1.7 clients can talk to 1.8 MetaMaster
  public static final String META_MASTER_CONFIG_SERVICE_NAME = "MetaMaster";
  public static final String META_MASTER_CLIENT_SERVICE_NAME = "MetaMaster";
  public static final String META_MASTER_MASTER_SERVICE_NAME = "MetaMasterMaster";
  public static final String METRICS_MASTER_CLIENT_SERVICE_NAME = "MetricsMasterClient";
  public static final String BLOCK_WORKER_CLIENT_SERVICE_NAME = "BlockWorkerClient";
  public static final String FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME = "FileSystemWorkerClient";
  public static final String JOURNAL_MASTER_CLIENT_SERVICE_NAME = "JournalMaster";
  public static final String RAFT_JOURNAL_SERVICE_NAME = "RaftJournal";

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
  public static final String ALLUXIO_LOCALITY_SCRIPT = "alluxio-locality.sh";

  public static final String SWIFT_AUTH_KEYSTONE = "keystone";
  public static final String SWIFT_AUTH_KEYSTONE_V3 = "keystonev3";
  public static final String SWIFT_AUTH_SWIFTAUTH = "swiftauth";

  public static final String LOCALITY_NODE = "node";
  public static final String LOCALITY_RACK = "rack";
  public static final String MESOS_LOCAL_INSTALL = "LOCAL";

  // Time-to-live
  public static final long NO_TTL = -1;

  // Security
  public static final int DEFAULT_FILE_SYSTEM_UMASK = 0022;
  public static final short DEFAULT_FILE_SYSTEM_MODE = (short) 0777;
  public static final short FILE_DIR_PERMISSION_DIFF = (short) 0111;
  public static final short INVALID_MODE = -1;

  public static final String IMPERSONATION_HDFS_USER = "_HDFS_USER_";
  public static final String IMPERSONATION_NONE = "_NONE_";

  public static final String MODE_BITS_NONE = "---";
  public static final String MODE_BITS_EXECUTE = "--x";
  public static final String MODE_BITS_WRITE = "-w-";
  public static final String MODE_BITS_WRITE_EXECUTE = "-wx";
  public static final String MODE_BITS_READ = "r--";
  public static final String MODE_BITS_READ_EXECUTE = "r-x";
  public static final String MODE_BITS_READ_WRITE = "rw-";
  public static final String MODE_BITS_ALL = "rwx";

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

  // Output stream flushed signal
  public static final String FLUSHED_SIGNAL = "FLUSHED";

  // Job service
  public static final String JOB_MASTER_WORKER_SERVICE_NAME = "JobMasterWorker";
  public static final long JOB_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final String JOB_MASTER_NAME = "JobMaster";
  public static final String JOB_MASTER_CLIENT_SERVICE_NAME = "JobMasterClient";
  public static final int JOB_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final String JOB_WORKER_NAME = "JobWorker";

  public static final int JOB_DEFAULT_MASTER_PORT = 20001;
  public static final int JOB_DEFAULT_MASTER_WEB_PORT = JOB_DEFAULT_MASTER_PORT + 1;
  public static final int JOB_DEFAULT_WORKER_PORT = 30001;
  public static final int JOB_DEFAULT_WORKER_DATA_PORT = JOB_DEFAULT_WORKER_PORT + 1;
  public static final int JOB_DEFAULT_WORKER_WEB_PORT = JOB_DEFAULT_WORKER_PORT + 2;

  // Journal
  public static final String JOB_JOURNAL_NAME = "JobJournal";

  // Replication
  public static final int REPLICATION_MAX_INFINITY = -1;

  // Persistence
  // The file should only be persisted after rename operation or persist CLI
  public static final int NO_AUTO_PERSIST = -1;
  public static final int PERSISTENCE_INVALID_JOB_ID = -1;
  public static final String PERSISTENCE_INVALID_UFS_PATH = "";

  // Table service
  public static final String TABLE_MASTER_NAME = "TableMaster";
  public static final String TABLE_MASTER_CLIENT_SERVICE_NAME = "TableMasterClient";
  public static final long TABLE_MASTER_CLIENT_SERVICE_VERSION = 1;

  // Medium name
  public static final String MEDIUM_MEM = "MEM";
  public static final String MEDIUM_HDD = "HDD";
  public static final String MEDIUM_SSD = "SSD";

  private Constants() {} // prevent instantiation
}
