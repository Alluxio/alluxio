/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon;

/**
 * System wide constants
 */
public class Constants {
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

  public static final int SECOND_MS = 1000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;

  public static final String SCHEME = "tachyon";
  public static final String HEADER = SCHEME + "://";

  public static final String SCHEME_FT = "tachyon-ft";
  public static final String HEADER_FT = SCHEME_FT + "://";

  public static final String HEADER_S3N = "s3n://";

  public static final int DEFAULT_MASTER_PORT = 19998;
  public static final int DEFAULT_MASTER_WEB_PORT = DEFAULT_MASTER_PORT + 1;
  public static final int DEFAULT_WORKER_PORT = 29998;
  public static final int DEFAULT_WORKER_DATA_SERVER_PORT = DEFAULT_WORKER_PORT + 1;
  public static final int DEFAULT_WORKER_WEB_PORT = DEFAULT_WORKER_PORT + 2;

  public static final int DEFAULT_MASTER_MAX_WORKER_THREADS = 2048;
  public static final int DEFAULT_WORKER_MAX_WORKER_THREADS = 2048;

  public static final int DEFAULT_USER_FAILED_SPACE_REQUEST_LIMITS = 3;

  public static final boolean DEFAULT_USER_ENABLE_LOCAL_READ = true;
  public static final boolean DEFAULT_USER_ENABLE_LOCAL_WRITE = true;

  public static final int DEFAULT_BLOCK_SIZE_BYTE = 512 * MB;

  public static final int DEFAULT_CHECKPOINT_CAP_MB_SEC = 1000;

  public static final int WORKER_BLOCKS_QUEUE_SIZE = 10000;

  public static final int DEFAULT_HOST_RESOLUTION_TIMEOUT_MS = 5000;

  /**
   * Version 1 [Before 0.5.0] Customized ser/de based. <br>
   * Version 2 [0.5.0] Starts to use JSON. <br>
   * Version 3 [0.6.0] Add lastModificationTimeMs to inode.
   */
  public static final int JOURNAL_VERSION = 3;

  // Configurations properties constants.
  // Please check and update Configuration-Settings.md file when you change or add Tachyon
  // configuration properties.

  // This constant is being used only in Hadoop MR job submissions where client need to pass site
  // specific configuration properties. It will be used as key in the MR Configuration.
  public static final String TACHYON_CONF_SITE = "tachyon.conf.site";

  public static final String DEFAULT_HOME = "/mnt/tachyon_default_home";
  public static final String DEFAULT_DATA_FOLDER = "/tachyon/data";
  public static final String DEFAULT_JOURNAL_FOLDER = DEFAULT_HOME + "/journal/";
  public static final String[] DEFAULT_STORAGE_TIER_DIR_QUOTA = "512MB,64GB,1TB".split(",");

  public static final String TACHYON_HOME = "tachyon.home";
  public static final String TACHYON_DEBUG = "tachyon.debug";
  public static final String TACHYON_LOGGER_TYPE = "tachyon.logger.type";
  public static final String WEB_RESOURCES = "tachyon.web.resources";
  public static final String WEB_THREAD_COUNT = "tachyon.web.threads";
  public static final String LOGS_DIR = "tachyon.logs.dir";
  public static final String UNDERFS_ADDRESS = "tachyon.underfs.address";
  public static final String UNDERFS_DATA_FOLDER = "tachyon.data.folder";
  public static final String UNDERFS_WORKERS_FOLDER = "tachyon.workers.folder";
  public static final String UNDERFS_HDFS_IMPL = "tachyon.underfs.hdfs.impl";
  public static final String UNDERFS_HADOOP_CONFIGURATION = "tachyon.underfs.hadoop.configuration";
  public static final String ASYNC_ENABLED = "tachyon.async.enabled";
  public static final String MAX_COLUMNS = "tachyon.max.columns";
  public static final String IN_TEST_MODE = "tachyon.test.mode";
  public static final String HOST_RESOLUTION_TIMEOUT_MS = "tachyon.host.resolution.timeout.ms";
  public static final String UNDERFS_GLUSTERFS_IMPL = "tachyon.underfs.glusterfs.impl";
  public static final String UNDERFS_GLUSTERFS_VOLUMES = "tachyon.underfs.glusterfs.volumes";
  public static final String UNDERFS_GLUSTERFS_MOUNTS = "tachyon.underfs.glusterfs.mounts";
  public static final String UNDERFS_GLUSTERFS_MR_DIR =
      "tachyon.underfs.glusterfs.mapred.system.dir";
  public static final String USE_ZOOKEEPER = "tachyon.usezookeeper";
  public static final String ZOOKEEPER_ADDRESS = "tachyon.zookeeper.address";
  public static final String ZOOKEEPER_ELECTION_PATH = "tachyon.zookeeper.election.path";
  public static final String ZOOKEEPER_LEADER_PATH = "tachyon.zookeeper.leader.path";
  public static final String UNDERFS_HADOOP_PREFIXS = "tachyon.underfs.hadoop.prefixes";
  public static final String MAX_TABLE_METADATA_BYTE = "tachyon.max.table.metadata.byte";
  public static final String METRICS_CONF_FILE = "tachyon.metrics.conf.file";
  public static final String FORMAT_FILE_PREFIX = "_format_";

  public static final String MASTER_FORMAT_FILE_PREFIX = "tachyon.master.format.file_prefix";
  public static final String MASTER_HOSTNAME = "tachyon.master.hostname";
  public static final String MASTER_HOSTNAME_LISTENING = "tachyon.master.hostname.listening";
  public static final String MASTER_HOSTNAME_LISTENING_WILDCARD = "*";
  public static final String MASTER_JOURNAL_FOLDER = "tachyon.master.journal.folder";
  public static final String MASTER_PORT = "tachyon.master.port";
  public static final String MASTER_ADDRESS = "tachyon.master.address";
  public static final String MASTER_WEB_PORT = "tachyon.master.web.port";
  public static final String MASTER_TEMPORARY_FOLDER = "tachyon.master.temporary.folder";
  public static final String MASTER_HEARTBEAT_INTERVAL_MS = "tachyon.master.heartbeat.interval.ms";
  public static final String MASTER_MAX_WORKER_THREADS = "tachyon.master.max.worker.threads";
  public static final String MASTER_MIN_WORKER_THREADS = "tachyon.master.min.worker.threads";
  public static final String MASTER_WORKER_TIMEOUT_MS = "tachyon.master.worker.timeout.ms";
  public static final String MASTER_WHITELIST = "tachyon.master.whitelist";
  public static final String MASTER_KEYTAB_KEY = "tachyon.master.keytab.file";
  public static final String MASTER_PRINCIPAL_KEY = "tachyon.master.principal";
  public static final String MASTER_RETRY_COUNT = "tachyon.master.retry";

  public static final String WORKER_MEMORY_SIZE = "tachyon.worker.memory.size";
  public static final String WORKER_PORT = "tachyon.worker.port";
  public static final String WORKER_DATA_PORT = "tachyon.worker.data.port";
  public static final String WORKER_WEB_PORT = "tachyon.worker.web.port";
  public static final String WORKER_DATA_FOLDER = "tachyon.worker.data.folder";
  public static final String WORKER_HEARTBEAT_TIMEOUT_MS = "tachyon.worker.heartbeat.timeout.ms";
  public static final String WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS =
      "tachyon.worker.to.master.heartbeat.interval.ms";
  public static final String WORKER_USER_TIMEOUT_MS = "tachyon.worker.user.timeout.ms";
  public static final String WORKER_MAX_WORKER_THREADS = "tachyon.worker.max.worker.threads";
  public static final String WORKER_MIN_WORKER_THREADS = "tachyon.worker.min.worker.threads";
  public static final String WORKER_CHECKPOINT_THREADS = "tachyon.worker.checkpoint.threads";
  public static final String WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC =
      "tachyon.worker.per.thread.checkpoint.cap.mb.sec";
  public static final String WORKER_NETTY_BOSS_THREADS =
      "tachyon.worker.network.netty.boss.threads";
  public static final String WORKER_NETTY_WORKER_THREADS =
      "tachyon.worker.network.netty.worker.threads";
  public static final String WORKER_NETWORK_NETTY_CHANNEL = "tachyon.worker.network.netty.channel";
  public static final String WORKER_NETTY_FILE_TRANSFER_TYPE =
      "tachyon.worker.network.netty.file.transfer";
  public static final String WORKER_NETTY_WATERMARK_HIGH =
      "tachyon.worker.network.netty.watermark.high";
  public static final String WORKER_NETTY_WATERMARK_LOW =
      "tachyon.worker.network.netty.watermark.low";
  public static final String WORKER_NETTY_BACKLOG = "tachyon.worker.network.netty.backlog";
  public static final String WORKER_NETTY_SEND_BUFFER = "tachyon.worker.network.netty.buffer.send";
  public static final String WORKER_NETTY_RECEIVE_BUFFER =
      "tachyon.worker.network.netty.buffer.receive";
  public static final String WORKER_NETTY_SHUTDOWN_QUIET_PERIOD =
      "tachyon.worker.network.netty.shutdown.quiet.period";
  public static final String WORKER_NETTY_SHUTDOWN_TIMEOUT =
      "tachyon.worker.network.netty.shutdown.timeout";
  public static final String WORKER_EVICT_STRATEGY_TYPE = "tachyon.worker.evict.strategy";
  public static final String WORKER_ALLOCATE_STRATEGY_TYPE = "tachyon.worker.allocate.strategy";
  public static final String WORKER_MAX_TIERED_STORAGE_LEVEL =
      "tachyon.worker.tieredstore.level.max";
  /**
   * This format is used as a template to generate the property name for a given level.
   * e.g., {@code String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT =
      "tachyon.worker.tieredstore.level%d.alias";
  /**
   * This format is used as a template to generate the property name for a given level.
   * e.g., {@code String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT =
      "tachyon.worker.tieredstore.level%d.dirs.path";
  /**
   * This format is used as a template to generate the property name for a given level.
   * e.g., {@code String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT =
      "tachyon.worker.tieredstore.level%d.dirs.quota";
  public static final String WORKER_KEYTAB_KEY = "tachyon.worker.keytab.file";
  public static final String WORKER_PRINCIPAL_KEY = "tachyon.worker.principal";
  public static final String WORKER_USER_TEMP_RELATIVE_FOLDER = "users";
  public static final String WORKER_DATA_SERVER = "tachyon.worker.data.server.class";

  public static final String USER_FAILED_SPACE_REQUEST_LIMITS =
      "tachyon.user.failed.space.request.limits";
  public static final String USER_QUOTA_UNIT_BYTES = "tachyon.user.quota.unit.bytes";
  public static final String USER_FILE_BUFFER_BYTES = "tachyon.user.file.buffer.bytes";
  public static final String USER_HEARTBEAT_INTERVAL_MS = "tachyon.user.heartbeat.interval.ms";
  public static final String USER_DEFAULT_BLOCK_SIZE_BYTE = "tachyon.user.default.block.size.byte";
  public static final String USER_NETTY_WORKER_THREADS =
      "tachyon.user.network.netty.worker.threads";
  public static final String USER_NETTY_CHANNEL = "tachyon.user.network.netty.channel";
  public static final String USER_REMOTE_READ_BUFFER_SIZE_BYTE =
      "tachyon.user.remote.read.buffer.size.byte";
  public static final String USER_DEFAULT_WRITE_TYPE = "tachyon.user.file.writetype.default";
  public static final String USER_REMOTE_BLOCK_READER = "tachyon.user.remote.block.reader.class";
  public static final String USER_REMOTE_BLOCK_WRITER = "tachyon.user.remote.block.writer.class";
  public static final String USER_ENABLE_LOCAL_READ = "tachyon.user.localread.enable";
  public static final String USER_ENABLE_LOCAL_WRITE = "tachyon.user.localwrite.enable";

  public static final String S3_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  public static final String S3_SECRET_KEY = "fs.s3n.awsSecretAccessKey";

  public static final String MASTER_COLUMN_FILE_PREFIX = "COL_";

  public static final String LOGGER_TYPE = System.getProperty(TACHYON_LOGGER_TYPE, "");
  public static final boolean DEBUG = Boolean.valueOf(System.getProperty(TACHYON_DEBUG, "false"));

  public static final long CLIENT_METRICS_VERSION = 1L;
  public static final int CLIENT_METRICS_SIZE = 9;
  public static final int CLIENT_METRICS_VERSION_INDEX = 0;
  public static final int BLOCKS_READ_LOCAL_INDEX = 1;
  public static final int BLOCKS_READ_REMOTE_INDEX = 2;
  public static final int BLOCKS_WRITTEN_LOCAL_INDEX = 3;
  public static final int BYTES_READ_LOCAL_INDEX = 4;
  public static final int BYTES_READ_REMOTE_INDEX = 5;
  public static final int BYTES_READ_UFS_INDEX = 6;
  public static final int BYTES_WRITTEN_LOCAL_INDEX = 7;
  public static final int BYTES_WRITTEN_UFS_INDEX = 8;

}
