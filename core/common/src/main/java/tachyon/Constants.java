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

import javax.annotation.concurrent.ThreadSafe;

/**
 * System wide constants
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
  public static final String COMMAND_FORMAT_LS = LS_FORMAT_PERMISSION + LS_FORMAT_USER_NAME
      + LS_FORMAT_GROUP_NAME + LS_FORMAT_FILE_SIZE + LS_FORMAT_CREATE_TIME + LS_FORMAT_FILE_TYPE
      + LS_FORMAT_FILE_PATH + "%n";

  public static final String MESOS_RESOURCE_CPUS = "cpus";
  public static final String MESOS_RESOURCE_MEM = "mem";
  public static final String MESOS_RESOURCE_DISK = "disk";
  public static final String MESOS_RESOURCE_PORTS = "ports";

  public static final int SECOND_MS = 1000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;

  public static final int BYTES_IN_INTEGER = 4;

  public static final String SCHEME = "tachyon";
  public static final String HEADER = SCHEME + "://";

  public static final String SCHEME_FT = "tachyon-ft";
  public static final String HEADER_FT = SCHEME_FT + "://";

  public static final String HEADER_OSS = "oss://";

  public static final String HEADER_S3 = "s3://";
  public static final String HEADER_S3N = "s3n://";
  public static final String HEADER_SWIFT = "swift://";

  public static final int DEFAULT_MASTER_PORT = 19998;
  public static final int DEFAULT_MASTER_WEB_PORT = DEFAULT_MASTER_PORT + 1;
  public static final int DEFAULT_WORKER_PORT = 29998;
  public static final int DEFAULT_WORKER_DATA_PORT = DEFAULT_WORKER_PORT + 1;
  public static final int DEFAULT_WORKER_WEB_PORT = DEFAULT_WORKER_PORT + 2;

  public static final int DEFAULT_HOST_RESOLUTION_TIMEOUT_MS = 5000;

  // Service versions should be incremented every time a backwards incompatible change occurs.
  public static final long BLOCK_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long BLOCK_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final long BLOCK_WORKER_CLIENT_SERVICE_VERSION = 1;
  public static final long FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final long LINEAGE_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final long LINEAGE_MASTER_WORKER_SERVICE_VERSION = 1;
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
  public static final String BLOCK_WORKER_CLIENT_SERVICE_NAME = "BlockWorkerClient";
  public static final String KEY_VALUE_MASTER_CLIENT_SERVICE_NAME = "KeyValueMasterClient";
  public static final String KEY_VALUE_WORKER_CLIENT_SERVICE_NAME = "KeyValueWorkerClient";

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

  public static final String TACHYON_HOME = "tachyon.home";
  public static final String TACHYON_DEBUG = "tachyon.debug";
  public static final String TACHYON_LOGGER_TYPE = "tachyon.logger.type";
  public static final String TACHYON_ACCESS_LOGGER_TYPE = "tachyon.accesslogger.type";
  public static final String TACHYON_VERSION = "tachyon.version";
  public static final String WEB_RESOURCES = "tachyon.web.resources";
  public static final String WEB_THREAD_COUNT = "tachyon.web.threads";
  public static final String LOGS_DIR = "tachyon.logs.dir";
  public static final String UNDERFS_ADDRESS = "tachyon.underfs.address";
  public static final String UNDERFS_HDFS_IMPL = "tachyon.underfs.hdfs.impl";
  public static final String UNDERFS_HDFS_CONFIGURATION = "tachyon.underfs.hdfs.configuration";
  public static final String UNDERFS_HDFS_PREFIXS = "tachyon.underfs.hdfs.prefixes";
  public static final String IN_TEST_MODE = "tachyon.test.mode";
  public static final String NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      "tachyon.network.host.resolution.timeout.ms";
  public static final String UNDERFS_GLUSTERFS_IMPL = "tachyon.underfs.glusterfs.impl";
  public static final String UNDERFS_GLUSTERFS_VOLUMES = "tachyon.underfs.glusterfs.volumes";
  public static final String UNDERFS_GLUSTERFS_MOUNTS = "tachyon.underfs.glusterfs.mounts";
  public static final String UNDERFS_GLUSTERFS_MR_DIR =
      "tachyon.underfs.glusterfs.mapred.system.dir";
  public static final String UNDERFS_OSS_CONNECT_MAX = "tachyon.underfs.oss.connection.max";
  public static final String UNDERFS_OSS_CONNECT_TIMEOUT =
      "tachyon.underfs.oss.connection.timeout.ms";
  public static final String UNDERFS_OSS_CONNECT_TTL = "tachyon.underfs.oss.connection.ttl";
  public static final String UNDERFS_OSS_SOCKET_TIMEOUT = "tachyon.underfs.oss.socket.timeout.ms";
  public static final String UNDERFS_S3_PROXY_HOST = "tachyon.underfs.s3.proxy.host";
  public static final String UNDERFS_S3_PROXY_PORT = "tachyon.underfs.s3.proxy.port";
  public static final String UNDERFS_S3_PROXY_HTTPS_ONLY = "tachyon.underfs.s3.proxy.https.only";
  public static final String ZOOKEEPER_ENABLED = "tachyon.zookeeper.enabled";
  public static final String ZOOKEEPER_ADDRESS = "tachyon.zookeeper.address";
  public static final String ZOOKEEPER_ELECTION_PATH = "tachyon.zookeeper.election.path";
  public static final String ZOOKEEPER_LEADER_PATH = "tachyon.zookeeper.leader.path";
  public static final String ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      "tachyon.zookeeper.leader.inquiry.retry";
  public static final String KEY_VALUE_ENABLED = "tachyon.keyvalue.enabled";
  public static final String KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
      "tachyon.keyvalue.partition.size.bytes.max";
  public static final String METRICS_CONF_FILE = "tachyon.metrics.conf.file";
  public static final String FORMAT_FILE_PREFIX = "_format_";

  public static final String INTEGRATION_MASTER_RESOURCE_CPU =
      "tachyon.integration.master.resource.cpu";
  public static final String INTEGRATION_MASTER_RESOURCE_MEM =
      "tachyon.integration.master.resource.mem";
  public static final String INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
      "tachyon.integration.yarn.workers.per.host.max";
  public static final String INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH =
      "tachyon.integration.mesos.executor.dependency.path";
  public static final String INTEGRATION_MESOS_JRE_PATH =
      "tachyon.integration.mesos.jre.path";
  public static final String INTEGRATION_MESOS_JRE_URL = "tachyon.integration.mesos.jre.url";
  public static final String INTEGRATION_MESOS_PRINCIPAL =
      "tachyon.integration.mesos.principal";
  public static final String INTEGRATION_MESOS_ROLE =
      "tachyon.integration.mesos.role";
  public static final String INTEGRATION_MESOS_SECRET =
      "tachyon.integration.mesos.secret";
  public static final String INTEGRATION_MESOS_TACHYON_MASTER_NAME =
      "tachyon.integration.mesos.master.name";
  public static final String INTEGRATION_MESOS_TACHYON_WORKER_NAME =
      "tachyon.integration.mesos.worker.name";
  public static final String INTEGRATION_MESOS_TACHYON_MASTER_NODE_COUNT =
      "tachyon.integration.mesos.master.node.count";
  public static final String INTEGRATION_MESOS_USER =
      "tachyon.integration.mesos.user";
  public static final String INTEGRATION_WORKER_RESOURCE_CPU =
      "tachyon.integration.worker.resource.cpu";
  public static final String INTEGRATION_WORKER_RESOURCE_MEM =
      "tachyon.integration.worker.resource.mem";

  public static final String MASTER_FORMAT_FILE_PREFIX = "tachyon.master.format.file_prefix";
  public static final String MASTER_JOURNAL_FOLDER = "tachyon.master.journal.folder";
  public static final String MASTER_JOURNAL_FORMATTER_CLASS =
      "tachyon.master.journal.formatter.class";
  public static final String MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
      "tachyon.master.journal.tailer.shutdown.quiet.wait.time.ms";
  public static final String MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      "tachyon.master.journal.tailer.sleep.time.ms";
  public static final String MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
      "tachyon.master.journal.log.size.bytes.max";

  public static final String MASTER_HOSTNAME = "tachyon.master.hostname";
  public static final String MASTER_BIND_HOST = "tachyon.master.bind.host";
  public static final String MASTER_RPC_PORT = "tachyon.master.port";
  public static final String MASTER_ADDRESS = "tachyon.master.address";
  public static final String MASTER_WEB_HOSTNAME = "tachyon.master.web.hostname";
  public static final String MASTER_WEB_BIND_HOST = "tachyon.master.web.bind.host";
  public static final String MASTER_WEB_PORT = "tachyon.master.web.port";
  public static final String MASTER_HEARTBEAT_INTERVAL_MS = "tachyon.master.heartbeat.interval.ms";
  public static final String MASTER_TTLCHECKER_INTERVAL_MS =
      "tachyon.master.ttlchecker.interval.ms";
  public static final String MASTER_WORKER_THREADS_MAX = "tachyon.master.worker.threads.max";
  public static final String MASTER_WORKER_THREADS_MIN = "tachyon.master.worker.threads.min";
  public static final String MASTER_WORKER_TIMEOUT_MS = "tachyon.master.worker.timeout.ms";
  public static final String MASTER_WHITELIST = "tachyon.master.whitelist";
  public static final String MASTER_KEYTAB_KEY = "tachyon.master.keytab.file";
  public static final String MASTER_PRINCIPAL_KEY = "tachyon.master.principal";
  public static final String MASTER_RETRY_COUNT = "tachyon.master.retry";
  public static final String MASTER_LINEAGE_CHECKPOINT_CLASS =
      "tachyon.master.lineage.checkpoint.class";
  public static final String MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS =
      "tachyon.master.lineage.checkpoint.interval.ms";
  public static final String MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS =
      "tachyon.master.lineage.recompute.interval.ms";
  public static final String MASTER_LINEAGE_RECOMPUTE_LOG_PATH =
      "tachyon.master.lineage.recompute.log.path";
  public static final String MASTER_TIERED_STORE_GLOBAL_LEVELS =
      "tachyon.master.tieredstore.global.levels";
  public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT =
      "tachyon.master.tieredstore.global.level%d.alias";

  public static final String WORKER_MEMORY_SIZE = "tachyon.worker.memory.size";
  public static final String WORKER_HOSTNAME = "tachyon.worker.hostname";
  public static final String WORKER_BIND_HOST = "tachyon.worker.bind.host";
  public static final String WORKER_RPC_PORT = "tachyon.worker.port";
  public static final String WORKER_DATA_HOSTNAME = "tachyon.worker.data.hostname";
  public static final String WORKER_DATA_BIND_HOST = "tachyon.worker.data.bind.host";
  public static final String WORKER_DATA_PORT = "tachyon.worker.data.port";
  public static final String WORKER_WEB_HOSTNAME = "tachyon.worker.web.hostname";
  public static final String WORKER_WEB_BIND_HOST = "tachyon.worker.web.bind.host";
  public static final String WORKER_WEB_PORT = "tachyon.worker.web.port";
  public static final String WORKER_DATA_FOLDER = "tachyon.worker.data.folder";
  public static final String WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      "tachyon.worker.block.heartbeat.timeout.ms";
  public static final String WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      "tachyon.worker.block.heartbeat.interval.ms";
  public static final String WORKER_SESSION_TIMEOUT_MS = "tachyon.worker.session.timeout.ms";
  public static final String WORKER_WORKER_BLOCK_THREADS_MAX = "tachyon.worker.block.threads.max";
  public static final String WORKER_WORKER_BLOCK_THREADS_MIN = "tachyon.worker.block.threads.min";
  public static final String WORKER_NETWORK_NETTY_BOSS_THREADS =
      "tachyon.worker.network.netty.boss.threads";
  public static final String WORKER_NETWORK_NETTY_WORKER_THREADS =
      "tachyon.worker.network.netty.worker.threads";
  public static final String WORKER_NETWORK_NETTY_CHANNEL = "tachyon.worker.network.netty.channel";
  public static final String WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
      "tachyon.worker.network.netty.file.transfer";
  public static final String WORKER_NETWORK_NETTY_WATERMARK_HIGH =
      "tachyon.worker.network.netty.watermark.high";
  public static final String WORKER_NETWORK_NETTY_WATERMARK_LOW =
      "tachyon.worker.network.netty.watermark.low";
  public static final String WORKER_NETWORK_NETTY_BACKLOG = "tachyon.worker.network.netty.backlog";
  public static final String WORKER_NETWORK_NETTY_BUFFER_SEND =
      "tachyon.worker.network.netty.buffer.send";
  public static final String WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
      "tachyon.worker.network.netty.buffer.receive";
  public static final String WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
      "tachyon.worker.network.netty.shutdown.quiet.period";
  public static final String WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
      "tachyon.worker.network.netty.shutdown.timeout";
  public static final String WORKER_ALLOCATOR_CLASS = "tachyon.worker.allocator.class";
  public static final String WORKER_EVICTOR_CLASS = "tachyon.worker.evictor.class";
  public static final String WORKER_EVICTOR_LRFU_STEP_FACTOR =
      "tachyon.worker.evictor.lrfu.step.factor";
  public static final String WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
      "tachyon.worker.evictor.lrfu.attenuation.factor";
  public static final String WORKER_TIERED_STORE_LEVELS =
      "tachyon.worker.tieredstore.levels";
  public static final String WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
      "tachyon.worker.filesystem.heartbeat.interval.ms";
  public static final String WORKER_FILE_PERSIST_POOL_SIZE =
      "tachyon.worker.file.persist.pool.size";

  public static final String WORKER_TIERED_STORE_BLOCK_LOCKS =
      "tachyon.worker.tieredstore.block.locks";
  /**
   * This format is used as a template to generate the property name for a given level. e.g.,
   * {@code String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT =
      "tachyon.worker.tieredstore.level%d.alias";
  /**
   * This format is used as a template to generate the property name for a given level. e.g.,
   * {@code String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT =
      "tachyon.worker.tieredstore.level%d.dirs.path";
  /**
   * This format is used as a template to generate the property name for a given level. e.g.,
   * {@code String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT =
      "tachyon.worker.tieredstore.level%d.dirs.quota";
  /**
   * This format is used as a template to generate the property name for a given level. e.g.,
   * {@code String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, level)}
   */
  public static final String WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT =
      "tachyon.worker.tieredstore.level%d.reserved.ratio";

  public static final String WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
      "tachyon.worker.tieredstore.reserver.interval.ms";

  public static final String WORKER_TIERED_STORE_RESERVER_ENABLED =
      "tachyon.worker.tieredstore.reserver.enabled";
  public static final String WORKER_KEYTAB_KEY = "tachyon.worker.keytab.file";
  public static final String WORKER_PRINCIPAL_KEY = "tachyon.worker.principal";
  public static final String WORKER_DATA_SERVER = "tachyon.worker.data.server.class";

  public static final String USER_FAILED_SPACE_REQUEST_LIMITS =
      "tachyon.user.failed.space.request.limits";
  public static final String USER_FILE_BUFFER_BYTES = "tachyon.user.file.buffer.bytes";
  public static final String USER_HEARTBEAT_INTERVAL_MS = "tachyon.user.heartbeat.interval.ms";
  public static final String USER_BLOCK_SIZE_BYTES_DEFAULT =
      "tachyon.user.block.size.bytes.default";
  public static final String USER_NETWORK_NETTY_WORKER_THREADS =
      "tachyon.user.network.netty.worker.threads";
  public static final String USER_NETWORK_NETTY_CHANNEL = "tachyon.user.network.netty.channel";
  public static final String USER_NETWORK_NETTY_TIMEOUT_MS =
      "tachyon.user.network.netty.timeout.ms";
  public static final String USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
      "tachyon.user.block.remote.read.buffer.size.bytes";
  public static final String USER_FILE_WRITE_TYPE_DEFAULT = "tachyon.user.file.writetype.default";
  public static final String USER_FILE_READ_TYPE_DEFAULT = "tachyon.user.file.readtype.default";
  public static final String USER_FILE_WRITE_LOCATION_POLICY =
      "tachyon.user.file.write.location.policy.class";
  public static final String USER_BLOCK_REMOTE_READER =
      "tachyon.user.block.remote.reader.class";
  public static final String USER_BLOCK_REMOTE_WRITER =
      "tachyon.user.block.remote.writer.class";
  public static final String USER_BLOCK_WORKER_CLIENT_THREADS =
      "tachyon.user.block.worker.client.threads";
  public static final String USER_BLOCK_MASTER_CLIENT_THREADS =
      "tachyon.user.block.master.client.threads";
  public static final String USER_FILE_MASTER_CLIENT_THREADS =
      "tachyon.user.file.master.client.threads";
  public static final String USER_LINEAGE_MASTER_CLIENT_THREADS =
      "tachyon.user.lineage.master.client.threads";
  public static final String USER_LINEAGE_ENABLED = "tachyon.user.lineage.enabled";

  public static final String USER_FILE_WAITCOMPLETED_POLL_MS =
      "tachyon.user.file.waitcompleted.poll.ms";

  /** tachyon-fuse related conf keys */

  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0).
   */
  public static final String FUSE_MAXWRITE_BYTES = "tachyon.fuse.maxwrite.bytes";
  /** Have the fuse process log every FS request */
  public static final String FUSE_DEBUG_ENABLE = "tachyon.fuse.debug.enabled";
  /** Maxium number of Tachyon Paths to cache for fuse conversion */
  public static final String FUSE_PATHCACHE_SIZE = "tachyon.fuse.cachedpaths.max";
  public static final String FUSE_DEFAULT_MOUNTPOINT = "tachyon.fuse.mount.default";
  public static final String FUSE_FS_ROOT = "tachyon.fuse.fs.root";
  /** FUSE file system name */
  public static final String FUSE_FS_NAME = "tachyon.fuse.fs.name";

  public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
  public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
  public static final String OSS_ENDPOINT_KEY = "fs.oss.endpoint";

  public static final String S3_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  public static final String S3_SECRET_KEY = "fs.s3n.awsSecretAccessKey";

  public static final String SWIFT_USER_KEY = "fs.swift.user";
  public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
  public static final String SWIFT_API_KEY = "fs.swift.apikey";
  public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
  public static final String SWIFT_AUTH_PORT_KEY = "fs.swift.auth.port";
  public static final String SWIFT_AUTH_METHOD_KEY = "fs.swift.auth.method";
  public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";

  public static final String MASTER_COLUMN_FILE_PREFIX = "COL_";

  public static final String LOGGER_TYPE = System.getProperty(TACHYON_LOGGER_TYPE, "");
  public static final String ACCESS_LOGGER_TYPE =
      System.getProperty(TACHYON_ACCESS_LOGGER_TYPE, "");
  public static final boolean DEBUG =
      Boolean.parseBoolean(System.getProperty(TACHYON_DEBUG, "false"));

  public static final long CLIENT_METRICS_VERSION = 1L;
  public static final int CLIENT_METRICS_SIZE = 11;
  public static final int CLIENT_METRICS_VERSION_INDEX = 0;
  public static final int BLOCKS_READ_LOCAL_INDEX = 1;
  public static final int BLOCKS_READ_REMOTE_INDEX = 2;
  public static final int BLOCKS_WRITTEN_LOCAL_INDEX = 3;
  public static final int BLOCKS_WRITTEN_REMOTE_INDEX = 4;
  public static final int BYTES_READ_LOCAL_INDEX = 5;
  public static final int BYTES_READ_REMOTE_INDEX = 6;
  public static final int BYTES_READ_UFS_INDEX = 7;
  public static final int BYTES_WRITTEN_LOCAL_INDEX = 8;
  public static final int BYTES_WRITTEN_REMOTE_INDEX = 9;
  public static final int BYTES_WRITTEN_UFS_INDEX = 10;

  /**
   * Maximum number of seconds to wait for thrift servers to stop on shutdown. Tests use a value of
   * 0 instead of this value so that they can run faster.
   */
  public static final int THRIFT_STOP_TIMEOUT_SECONDS = 60;

  // ttl related
  public static final long NO_TTL = -1;

  /** Security */
  // Authentication
  public static final String SECURITY_AUTHENTICATION_TYPE = "tachyon.security.authentication.type";
  public static final String SECURITY_AUTHENTICATION_CUSTOM_PROVIDER =
      "tachyon.security.authentication.custom.provider.class";
  public static final String SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS =
      "tachyon.security.authentication.socket.timeout.ms";
  public static final String SECURITY_LOGIN_USERNAME = "tachyon.security.login.username";
  // Authorization
  public static final String SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      "tachyon.security.authorization.permission.enabled";
  public static final String SECURITY_AUTHORIZATION_PERMISSIONS_UMASK =
      "tachyon.security.authorization.permission.umask";
  public static final String SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
      "tachyon.security.authorization.permission.supergroup";
  // Group Mapping
  public static final String SECURITY_GROUP_MAPPING = "tachyon.security.group.mapping.class";

  // Security related constant value
  public static final int DEFAULT_FS_PERMISSIONS_UMASK = 0022;
  public static final short DEFAULT_FS_FULL_PERMISSION = (short) 0777;
  public static final short FILE_DIR_PERMISSION_DIFF = (short) 0111;
  public static final short INVALID_PERMISSION = -1;

  private Constants() {} // prevent instantiation
}
