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

import java.util.HashMap;
import java.util.Map;

/**
 * Configurations properties constants. Please check and update Configuration-Settings.md file when
 * you change or add Alluxio configuration properties.
 */
public enum PropertyKey {
  DEBUG(Constants.DEBUG),
  HOME(Constants.HOME),
  INTEGRATION_MASTER_RESOURCE_CPU(Constants.INTEGRATION_MASTER_RESOURCE_CPU),
  INTEGRATION_MASTER_RESOURCE_MEM(Constants.INTEGRATION_MASTER_RESOURCE_MEM),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NAME(Constants.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT(
      Constants.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT),
  INTEGRATION_MESOS_ALLUXIO_WORKER_NAME(Constants.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME),
  INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH(Constants.INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH),
  INTEGRATION_MESOS_JRE_PATH(Constants.INTEGRATION_MESOS_JRE_PATH),
  INTEGRATION_MESOS_JRE_URL(Constants.INTEGRATION_MESOS_JRE_URL),
  INTEGRATION_MESOS_PRINCIPAL(Constants.INTEGRATION_MESOS_PRINCIPAL),
  INTEGRATION_MESOS_ROLE(Constants.INTEGRATION_MESOS_ROLE),
  INTEGRATION_MESOS_SECRET(Constants.INTEGRATION_MESOS_SECRET),
  INTEGRATION_MESOS_USER(Constants.INTEGRATION_MESOS_USER),
  INTEGRATION_WORKER_RESOURCE_CPU(Constants.INTEGRATION_WORKER_RESOURCE_CPU),
  INTEGRATION_WORKER_RESOURCE_MEM(Constants.INTEGRATION_WORKER_RESOURCE_MEM),
  INTEGRATION_YARN_WORKERS_PER_HOST_MAX(Constants.INTEGRATION_YARN_WORKERS_PER_HOST_MAX),
  KEY_VALUE_ENABLED(Constants.KEY_VALUE_ENABLED),
  KEY_VALUE_PARTITION_SIZE_BYTES_MAX(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX),
  LOGGER_TYPE(Constants.LOGGER_TYPE),
  LOGS_DIR(Constants.LOGS_DIR),
  METRICS_CONF_FILE(Constants.METRICS_CONF_FILE),
  NETWORK_HOST_RESOLUTION_TIMEOUT_MS(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS),
  NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX(Constants.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX),
  SITE_CONF_DIR(Constants.SITE_CONF_DIR),
  TEST_MODE(Constants.TEST_MODE),
  VERSION(Constants.VERSION),
  WEB_RESOURCES(Constants.WEB_RESOURCES),
  WEB_THREADS(Constants.WEB_THREADS),
  ZOOKEEPER_ADDRESS(Constants.ZOOKEEPER_ADDRESS),
  ZOOKEEPER_ELECTION_PATH(Constants.ZOOKEEPER_ELECTION_PATH),
  ZOOKEEPER_ENABLED(Constants.ZOOKEEPER_ENABLED),
  ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT(Constants.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT),
  ZOOKEEPER_LEADER_PATH(Constants.ZOOKEEPER_LEADER_PATH),

  //
  // UFS related properties
  //
  UNDERFS_ADDRESS(Constants.UNDERFS_ADDRESS),
  UNDERFS_GLUSTERFS_IMPL(Constants.UNDERFS_GLUSTERFS_IMPL),
  UNDERFS_GLUSTERFS_MOUNTS(Constants.UNDERFS_GLUSTERFS_MOUNTS),
  UNDERFS_GLUSTERFS_MR_DIR(Constants.UNDERFS_GLUSTERFS_MR_DIR),
  UNDERFS_GLUSTERFS_VOLUMES(Constants.UNDERFS_GLUSTERFS_VOLUMES),
  UNDERFS_HDFS_CONFIGURATION(Constants.UNDERFS_HDFS_CONFIGURATION),
  UNDERFS_HDFS_IMPL(Constants.UNDERFS_HDFS_IMPL),
  UNDERFS_HDFS_PREFIXES(Constants.UNDERFS_HDFS_PREFIXES),
  UNDERFS_OSS_CONNECT_MAX(Constants.UNDERFS_OSS_CONNECT_MAX),
  UNDERFS_OSS_CONNECT_TIMEOUT(Constants.UNDERFS_OSS_CONNECT_TIMEOUT),
  UNDERFS_OSS_CONNECT_TTL(Constants.UNDERFS_OSS_CONNECT_TTL),
  UNDERFS_OSS_SOCKET_TIMEOUT(Constants.UNDERFS_OSS_SOCKET_TIMEOUT),
  UNDERFS_S3_ADMIN_THREADS_MAX(Constants.UNDERFS_S3_ADMIN_THREADS_MAX),
  UNDERFS_S3_DISABLE_DNS_BUCKETS(Constants.UNDERFS_S3_DISABLE_DNS_BUCKETS),
  UNDERFS_S3_ENDPOINT(Constants.UNDERFS_S3_ENDPOINT),
  UNDERFS_S3_ENDPOINT_HTTP_PORT(Constants.UNDERFS_S3_ENDPOINT_HTTP_PORT),
  UNDERFS_S3_ENDPOINT_HTTPS_PORT(Constants.UNDERFS_S3_ENDPOINT_HTTPS_PORT),
  UNDERFS_S3_PROXY_HOST(Constants.UNDERFS_S3_PROXY_HOST),
  UNDERFS_S3_PROXY_HTTPS_ONLY(Constants.UNDERFS_S3_PROXY_HTTPS_ONLY),
  UNDERFS_S3_PROXY_PORT(Constants.UNDERFS_S3_PROXY_PORT),
  UNDERFS_S3_THREADS_MAX(Constants.UNDERFS_S3_THREADS_MAX),
  UNDERFS_S3_UPLOAD_THREADS_MAX(Constants.UNDERFS_S3_UPLOAD_THREADS_MAX),
  UNDERFS_S3A_SECURE_HTTP_ENABLED(Constants.UNDERFS_S3A_SECURE_HTTP_ENABLED),
  UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED(Constants.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED),
  UNDERFS_S3A_SOCKET_TIMEOUT_MS(Constants.UNDERFS_S3A_SOCKET_TIMEOUT_MS),

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  GCS_ACCESS_KEY(Constants.GCS_ACCESS_KEY),
  GCS_SECRET_KEY(Constants.GCS_SECRET_KEY),
  OSS_ACCESS_KEY(Constants.OSS_ACCESS_KEY),
  OSS_ENDPOINT_KEY(Constants.OSS_ENDPOINT_KEY),
  OSS_SECRET_KEY(Constants.OSS_SECRET_KEY),
  S3A_ACCESS_KEY(Constants.S3A_ACCESS_KEY),
  S3A_SECRET_KEY(Constants.S3A_SECRET_KEY),
  S3N_ACCESS_KEY(Constants.S3N_ACCESS_KEY),
  S3N_SECRET_KEY(Constants.S3N_SECRET_KEY),
  SWIFT_API_KEY(Constants.SWIFT_API_KEY),
  SWIFT_AUTH_METHOD_KEY(Constants.SWIFT_AUTH_METHOD_KEY),
  SWIFT_AUTH_PORT_KEY(Constants.SWIFT_AUTH_PORT_KEY),
  SWIFT_AUTH_URL_KEY(Constants.SWIFT_AUTH_URL_KEY),
  SWIFT_PASSWORD_KEY(Constants.SWIFT_PASSWORD_KEY),
  SWIFT_SIMULATION(Constants.SWIFT_SIMULATION),
  SWIFT_TENANT_KEY(Constants.SWIFT_TENANT_KEY),
  SWIFT_USE_PUBLIC_URI_KEY(Constants.SWIFT_USE_PUBLIC_URI_KEY),
  SWIFT_USER_KEY(Constants.SWIFT_USER_KEY),

  //
  // Master related properties
  //
  MASTER_ADDRESS(Constants.MASTER_ADDRESS),
  MASTER_BIND_HOST(Constants.MASTER_BIND_HOST),
  MASTER_FILE_ASYNC_PERSIST_HANDLER(Constants.MASTER_FILE_ASYNC_PERSIST_HANDLER),
  MASTER_FORMAT_FILE_PREFIX(Constants.MASTER_FORMAT_FILE_PREFIX),
  MASTER_HEARTBEAT_INTERVAL_MS(Constants.MASTER_HEARTBEAT_INTERVAL_MS),
  MASTER_HOSTNAME(Constants.MASTER_HOSTNAME),
  MASTER_JOURNAL_FLUSH_BATCH_TIME_MS(Constants.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS),
  MASTER_JOURNAL_FOLDER(Constants.MASTER_JOURNAL_FOLDER),
  MASTER_JOURNAL_FORMATTER_CLASS(Constants.MASTER_JOURNAL_FORMATTER_CLASS),
  MASTER_JOURNAL_LOG_SIZE_BYTES_MAX(Constants.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX),
  MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS(
      Constants.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS),
  MASTER_JOURNAL_TAILER_SLEEP_TIME_MS(Constants.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS),
  MASTER_KEYTAB_KEY_FILE(Constants.MASTER_KEYTAB_KEY_FILE),
  MASTER_LINEAGE_CHECKPOINT_CLASS(Constants.MASTER_LINEAGE_CHECKPOINT_CLASS),
  MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS(Constants.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS),
  MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS(Constants.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS),
  MASTER_LINEAGE_RECOMPUTE_LOG_PATH(Constants.MASTER_LINEAGE_RECOMPUTE_LOG_PATH),
  MASTER_PRINCIPAL(Constants.MASTER_PRINCIPAL),
  MASTER_RETRY(Constants.MASTER_RETRY),
  MASTER_RPC_PORT(Constants.MASTER_RPC_PORT),
  MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS),
  MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS),
  MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS),
  MASTER_TIERED_STORE_GLOBAL_LEVELS(Constants.MASTER_TIERED_STORE_GLOBAL_LEVELS),
  MASTER_TTL_CHECKER_INTERVAL_MS(Constants.MASTER_TTL_CHECKER_INTERVAL_MS),
  MASTER_WEB_BIND_HOST(Constants.MASTER_WEB_BIND_HOST),
  MASTER_WEB_HOSTNAME(Constants.MASTER_WEB_HOSTNAME),
  MASTER_WEB_PORT(Constants.MASTER_WEB_PORT),
  MASTER_WHITELIST(Constants.MASTER_WHITELIST),
  MASTER_WORKER_THREADS_MAX(Constants.MASTER_WORKER_THREADS_MAX),
  MASTER_WORKER_THREADS_MIN(Constants.MASTER_WORKER_THREADS_MIN),
  MASTER_WORKER_TIMEOUT_MS(Constants.MASTER_WORKER_TIMEOUT_MS),

  //
  // Worker related properties
  //
  WORKER_ALLOCATOR_CLASS(Constants.WORKER_ALLOCATOR_CLASS),
  WORKER_BIND_HOST(Constants.WORKER_BIND_HOST),
  WORKER_BLOCK_HEARTBEAT_INTERVAL_MS(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
  WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS(Constants.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS),
  WORKER_DATA_BIND_HOST(Constants.WORKER_DATA_BIND_HOST),
  WORKER_DATA_FOLDER(Constants.WORKER_DATA_FOLDER),
  WORKER_DATA_HOSTNAME(Constants.WORKER_DATA_HOSTNAME),
  WORKER_DATA_PORT(Constants.WORKER_DATA_PORT),
  WORKER_DATA_SERVER_CLASS(Constants.WORKER_DATA_SERVER_CLASS),
  WORKER_DATA_TMP_FOLDER(Constants.WORKER_DATA_TMP_FOLDER),
  WORKER_DATA_TMP_SUBDIR_MAX(Constants.WORKER_DATA_TMP_SUBDIR_MAX),
  WORKER_EVICTOR_CLASS(Constants.WORKER_EVICTOR_CLASS),
  WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR(Constants.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR),
  WORKER_EVICTOR_LRFU_STEP_FACTOR(Constants.WORKER_EVICTOR_LRFU_STEP_FACTOR),
  WORKER_FILE_PERSIST_POOL_SIZE(Constants.WORKER_FILE_PERSIST_POOL_SIZE),
  WORKER_FILE_PERSIST_RATE_LIMIT(Constants.WORKER_FILE_PERSIST_RATE_LIMIT),
  WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED(Constants.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED),
  WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS(Constants.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS),
  WORKER_HOSTNAME(Constants.WORKER_HOSTNAME),
  WORKER_KEYTAB_FILE(Constants.WORKER_KEYTAB_FILE),
  WORKER_MEMORY_SIZE(Constants.WORKER_MEMORY_SIZE),
  WORKER_NETWORK_NETTY_BACKLOG(Constants.WORKER_NETWORK_NETTY_BACKLOG),
  WORKER_NETWORK_NETTY_BOSS_THREADS(Constants.WORKER_NETWORK_NETTY_BOSS_THREADS),
  WORKER_NETWORK_NETTY_BUFFER_RECEIVE(Constants.WORKER_NETWORK_NETTY_BUFFER_RECEIVE),
  WORKER_NETWORK_NETTY_BUFFER_SEND(Constants.WORKER_NETWORK_NETTY_BUFFER_SEND),
  WORKER_NETWORK_NETTY_CHANNEL(Constants.WORKER_NETWORK_NETTY_CHANNEL),
  WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE(Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE),
  WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD),
  WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT),
  WORKER_NETWORK_NETTY_WATERMARK_HIGH(Constants.WORKER_NETWORK_NETTY_WATERMARK_HIGH),
  WORKER_NETWORK_NETTY_WATERMARK_LOW(Constants.WORKER_NETWORK_NETTY_WATERMARK_LOW),
  WORKER_NETWORK_NETTY_WORKER_THREADS(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS),
  WORKER_PRINCIPAL(Constants.WORKER_PRINCIPAL),
  WORKER_RPC_PORT(Constants.WORKER_RPC_PORT),
  WORKER_SESSION_TIMEOUT_MS(Constants.WORKER_SESSION_TIMEOUT_MS),
  WORKER_TIERED_STORE_BLOCK_LOCKS(Constants.WORKER_TIERED_STORE_BLOCK_LOCKS),
  WORKER_TIERED_STORE_LEVEL0_ALIAS(Constants.WORKER_TIERED_STORE_LEVEL0_ALIAS),
  WORKER_TIERED_STORE_LEVEL0_DIRS_PATH(Constants.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH),
  WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA(Constants.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA),
  WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO(Constants.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO),
  WORKER_TIERED_STORE_LEVEL1_ALIAS(Constants.WORKER_TIERED_STORE_LEVEL1_ALIAS),
  WORKER_TIERED_STORE_LEVEL1_DIRS_PATH(Constants.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH),
  WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA(Constants.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA),
  WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO(Constants.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO),
  WORKER_TIERED_STORE_LEVEL2_ALIAS(Constants.WORKER_TIERED_STORE_LEVEL2_ALIAS),
  WORKER_TIERED_STORE_LEVEL2_DIRS_PATH(Constants.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH),
  WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA(Constants.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA),
  WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO(Constants.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO),
  WORKER_TIERED_STORE_LEVELS(Constants.WORKER_TIERED_STORE_LEVELS),
  WORKER_TIERED_STORE_RESERVER_ENABLED(Constants.WORKER_TIERED_STORE_RESERVER_ENABLED),
  WORKER_TIERED_STORE_RESERVER_INTERVAL_MS(Constants.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS),
  WORKER_WEB_BIND_HOST(Constants.WORKER_WEB_BIND_HOST),
  WORKER_WEB_HOSTNAME(Constants.WORKER_WEB_HOSTNAME),
  WORKER_WEB_PORT(Constants.WORKER_WEB_PORT),
  WORKER_WORKER_BLOCK_THREADS_MAX(Constants.WORKER_WORKER_BLOCK_THREADS_MAX),
  WORKER_WORKER_BLOCK_THREADS_MIN(Constants.WORKER_WORKER_BLOCK_THREADS_MIN),

  //
  // User related properties
  //
  USER_BLOCK_MASTER_CLIENT_THREADS(Constants.USER_BLOCK_MASTER_CLIENT_THREADS),
  USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES),
  USER_BLOCK_REMOTE_READER(Constants.USER_BLOCK_REMOTE_READER),
  USER_BLOCK_REMOTE_WRITER(Constants.USER_BLOCK_REMOTE_WRITER),
  USER_BLOCK_SIZE_BYTES_DEFAULT(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT),
  USER_BLOCK_WORKER_CLIENT_THREADS(Constants.USER_BLOCK_WORKER_CLIENT_THREADS),
  USER_FAILED_SPACE_REQUEST_LIMITS(Constants.USER_FAILED_SPACE_REQUEST_LIMITS),
  USER_FILE_BUFFER_BYTES(Constants.USER_FILE_BUFFER_BYTES),
  USER_FILE_CACHE_PARTIALLY_READ_BLOCK(Constants.USER_FILE_CACHE_PARTIALLY_READ_BLOCK),
  USER_FILE_MASTER_CLIENT_THREADS(Constants.USER_FILE_MASTER_CLIENT_THREADS),
  USER_FILE_READ_TYPE_DEFAULT(Constants.USER_FILE_READ_TYPE_DEFAULT),
  USER_FILE_SEEK_BUFFER_SIZE_BYTES(Constants.USER_FILE_SEEK_BUFFER_SIZE_BYTES),
  USER_FILE_WAITCOMPLETED_POLL_MS(Constants.USER_FILE_WAITCOMPLETED_POLL_MS),
  USER_FILE_WORKER_CLIENT_THREADS(Constants.USER_FILE_WORKER_CLIENT_THREADS),
  USER_FILE_WRITE_LOCATION_POLICY(Constants.USER_FILE_WRITE_LOCATION_POLICY),
  USER_FILE_WRITE_TYPE_DEFAULT(Constants.USER_FILE_WRITE_TYPE_DEFAULT),
  USER_HEARTBEAT_INTERVAL_MS(Constants.USER_HEARTBEAT_INTERVAL_MS),
  USER_LINEAGE_ENABLED(Constants.USER_LINEAGE_ENABLED),
  USER_LINEAGE_MASTER_CLIENT_THREADS(Constants.USER_LINEAGE_MASTER_CLIENT_THREADS),
  USER_NETWORK_NETTY_CHANNEL(Constants.USER_NETWORK_NETTY_CHANNEL),
  USER_NETWORK_NETTY_TIMEOUT_MS(Constants.USER_NETWORK_NETTY_TIMEOUT_MS),
  USER_NETWORK_NETTY_WORKER_THREADS(Constants.USER_NETWORK_NETTY_WORKER_THREADS),
  USER_UFS_DELEGATION_ENABLED(Constants.USER_UFS_DELEGATION_ENABLED),
  USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES(Constants.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES),
  USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES(
      Constants.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES),

  //
  // FUSE integration related properties
  //
  /** Have the fuse process log every FS request. */
  FUSE_DEBUG_ENABLED(Constants.FUSE_DEBUG_ENABLED),
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  FUSE_CACHED_PATHS_MAX(Constants.FUSE_CACHED_PATHS_MAX),
  /** FUSE file system name. */
  FUSE_FS_NAME(Constants.FUSE_FS_NAME),
  FUSE_FS_ROOT(Constants.FUSE_FS_ROOT),
  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  FUSE_MAXWRITE_BYTES(Constants.FUSE_MAXWRITE_BYTES),
  FUSE_MOUNT_DEFAULT(Constants.FUSE_MOUNT_DEFAULT),

  //
  // Security related properties
  //
  SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS(
      Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS),
  SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS),
  SECURITY_AUTHENTICATION_TYPE(Constants.SECURITY_AUTHENTICATION_TYPE),
  SECURITY_AUTHORIZATION_PERMISSION_ENABLED(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED),
  SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP(
      Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP),
  SECURITY_AUTHORIZATION_PERMISSION_UMASK(Constants.SECURITY_AUTHORIZATION_PERMISSION_UMASK),
  SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS(Constants.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS),
  SECURITY_GROUP_MAPPING_CLASS(Constants.SECURITY_GROUP_MAPPING_CLASS),
  SECURITY_LOGIN_USERNAME(Constants.SECURITY_LOGIN_USERNAME),;

  public static class Constants {
    public static final String DEBUG = "alluxio.debug";
    public static final String FUSE_CACHED_PATHS_MAX = "alluxio.fuse.cached.paths.max";
    public static final String FUSE_DEBUG_ENABLED = "alluxio.fuse.debug.enabled";
    public static final String FUSE_FS_NAME = "alluxio.fuse.fs.name";
    public static final String FUSE_FS_ROOT = "alluxio.fuse.fs.root";
    public static final String FUSE_MAXWRITE_BYTES = "alluxio.fuse.maxwrite.bytes";
    public static final String FUSE_MOUNT_DEFAULT = "alluxio.fuse.mount.default";
    public static final String GCS_ACCESS_KEY = "fs.gcs.accessKeyId";
    public static final String GCS_SECRET_KEY = "fs.gcs.secretAccessKey";
    public static final String HOME = "alluxio.home";
    public static final String INTEGRATION_MASTER_RESOURCE_CPU =
        "alluxio.integration.master.resource.cpu";
    public static final String INTEGRATION_MASTER_RESOURCE_MEM =
        "alluxio.integration.master.resource.mem";
    public static final String INTEGRATION_MESOS_ALLUXIO_MASTER_NAME =
        "alluxio.integration.mesos.master.name";
    public static final String INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT =
        "alluxio.integration.mesos.master.node.count";
    public static final String INTEGRATION_MESOS_ALLUXIO_WORKER_NAME =
        "alluxio.integration.mesos.worker.name";
    public static final String INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH =
        "alluxio.integration.mesos.executor.dependency.path";
    public static final String INTEGRATION_MESOS_JRE_PATH = "alluxio.integration.mesos.jre.path";
    public static final String INTEGRATION_MESOS_JRE_URL = "alluxio.integration.mesos.jre.url";
    public static final String INTEGRATION_MESOS_PRINCIPAL = "alluxio.integration.mesos.principal";
    public static final String INTEGRATION_MESOS_ROLE = "alluxio.integration.mesos.role";
    public static final String INTEGRATION_MESOS_SECRET = "alluxio.integration.mesos.secret";
    public static final String INTEGRATION_MESOS_USER = "alluxio.integration.mesos.user";
    public static final String INTEGRATION_WORKER_RESOURCE_CPU =
        "alluxio.integration.worker.resource.cpu";
    public static final String INTEGRATION_WORKER_RESOURCE_MEM =
        "alluxio.integration.worker.resource.mem";
    public static final String INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
        "alluxio.integration.yarn.workers.per.host.max";
    public static final String KEY_VALUE_ENABLED = "alluxio.keyvalue.enabled";
    public static final String KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
        "alluxio.keyvalue.partition.size.bytes.max";
    public static final String LOGGER_TYPE = "alluxio.logger.type";
    public static final String LOGS_DIR = "alluxio.logs.dir";
    public static final String MASTER_ADDRESS = "alluxio.master.address";
    public static final String MASTER_BIND_HOST = "alluxio.master.bind.host";
    public static final String MASTER_FILE_ASYNC_PERSIST_HANDLER =
        "alluxio.master.file.async.persist.handler";
    public static final String MASTER_FORMAT_FILE_PREFIX = "alluxio.master.format.file_prefix";
    public static final String MASTER_HEARTBEAT_INTERVAL_MS =
        "alluxio.master.heartbeat.interval.ms";
    public static final String MASTER_HOSTNAME = "alluxio.master.hostname";
    public static final String MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
        "alluxio.master.journal.flush.batch.time.ms";
    public static final String MASTER_JOURNAL_FOLDER = "alluxio.master.journal.folder";
    public static final String MASTER_JOURNAL_FORMATTER_CLASS =
        "alluxio.master.journal.formatter.class";
    public static final String MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
        "alluxio.master.journal.log.size.bytes.max";
    public static final String MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
        "alluxio.master.journal.tailer.shutdown.quiet.wait.time.ms";
    public static final String MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
        "alluxio.master.journal.tailer.sleep.time.ms";
    public static final String MASTER_KEYTAB_KEY_FILE = "alluxio.master.keytab.file";
    public static final String MASTER_LINEAGE_CHECKPOINT_CLASS =
        "alluxio.master.lineage.checkpoint.class";
    public static final String MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS =
        "alluxio.master.lineage.checkpoint.interval.ms";
    public static final String MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS =
        "alluxio.master.lineage.recompute.interval.ms";
    public static final String MASTER_LINEAGE_RECOMPUTE_LOG_PATH =
        "alluxio.master.lineage.recompute.log.path";
    public static final String MASTER_PRINCIPAL = "alluxio.master.principal";
    public static final String MASTER_RETRY = "alluxio.master.retry";
    public static final String MASTER_RPC_PORT = "alluxio.master.port";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
        "alluxio.master.tieredstore.global.level0.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
        "alluxio.master.tieredstore.global.level1.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
        "alluxio.master.tieredstore.global.level2.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVELS =
        "alluxio.master.tieredstore.global.levels";
    public static final String MASTER_TTL_CHECKER_INTERVAL_MS =
        "alluxio.master.ttl.checker.interval.ms";
    public static final String MASTER_WEB_BIND_HOST = "alluxio.master.web.bind.host";
    public static final String MASTER_WEB_HOSTNAME = "alluxio.master.web.hostname";
    public static final String MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String MASTER_WHITELIST = "alluxio.master.whitelist";
    public static final String MASTER_WORKER_THREADS_MAX = "alluxio.master.worker.threads.max";
    public static final String MASTER_WORKER_THREADS_MIN = "alluxio.master.worker.threads.min";
    public static final String MASTER_WORKER_TIMEOUT_MS = "alluxio.master.worker.timeout.ms";
    public static final String METRICS_CONF_FILE = "alluxio.metrics.conf.file";
    public static final String NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
        "alluxio.network.host.resolution.timeout.ms";
    public static final String NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
        "alluxio.network.thrift.frame.size.bytes.max";
    public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    public static final String OSS_ENDPOINT_KEY = "fs.oss.endpoint";
    public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    public static final String S3A_ACCESS_KEY = "aws.accessKeyId";
    public static final String S3A_SECRET_KEY = "aws.secretKey";
    public static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
    public static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";
    public static final String SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
        "alluxio.security.authentication.custom.provider.class";
    public static final String SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS =
        "alluxio.security.authentication.socket.timeout.ms";
    public static final String SECURITY_AUTHENTICATION_TYPE =
        "alluxio.security.authentication.type";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
        "alluxio.security.authorization.permission.enabled";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
        "alluxio.security.authorization.permission.supergroup";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_UMASK =
        "alluxio.security.authorization.permission.umask";
    public static final String SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
        "alluxio.security.group.mapping.cache.timeout.ms";
    public static final String SECURITY_GROUP_MAPPING_CLASS =
        "alluxio.security.group.mapping.class";
    public static final String SECURITY_LOGIN_USERNAME = "alluxio.security.login.username";
    public static final String SITE_CONF_DIR = "alluxio.site.conf.dir";
    public static final String SWIFT_API_KEY = "fs.swift.apikey";
    public static final String SWIFT_AUTH_METHOD_KEY = "fs.swift.auth.method";
    public static final String SWIFT_AUTH_PORT_KEY = "fs.swift.auth.port";
    public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
    public static final String SWIFT_PASSWORD_KEY = "fs.swift.password";
    public static final String SWIFT_SIMULATION = "fs.swift.simulation";
    public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
    public static final String SWIFT_USER_KEY = "fs.swift.user";
    public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";
    public static final String TEST_MODE = "alluxio.test.mode";
    public static final String UNDERFS_ADDRESS = "alluxio.underfs.address";
    public static final String UNDERFS_GLUSTERFS_IMPL = "alluxio.underfs.glusterfs.impl";
    public static final String UNDERFS_GLUSTERFS_MOUNTS = "alluxio.underfs.glusterfs.mounts";
    public static final String UNDERFS_GLUSTERFS_MR_DIR =
        "alluxio.underfs.glusterfs.mapred.system.dir";
    public static final String UNDERFS_GLUSTERFS_VOLUMES = "alluxio.underfs.glusterfs.volumes";
    public static final String UNDERFS_HDFS_CONFIGURATION = "alluxio.underfs.hdfs.configuration";
    public static final String UNDERFS_HDFS_IMPL = "alluxio.underfs.hdfs.impl";
    public static final String UNDERFS_HDFS_PREFIXES = "alluxio.underfs.hdfs.prefixes";
    public static final String UNDERFS_OSS_CONNECT_MAX = "alluxio.underfs.oss.connection.max";
    public static final String UNDERFS_OSS_CONNECT_TIMEOUT =
        "alluxio.underfs.oss.connection.timeout.ms";
    public static final String UNDERFS_OSS_CONNECT_TTL = "alluxio.underfs.oss.connection.ttl";
    public static final String UNDERFS_OSS_SOCKET_TIMEOUT = "alluxio.underfs.oss.socket.timeout.ms";
    public static final String UNDERFS_S3A_SECURE_HTTP_ENABLED =
        "alluxio.underfs.s3a.secure.http.enabled";
    public static final String UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
        "alluxio.underfs.s3a.server.side.encryption.enabled";
    public static final String UNDERFS_S3A_SOCKET_TIMEOUT_MS =
        "alluxio.underfs.s3a.socket.timeout.ms";
    public static final String UNDERFS_S3_ADMIN_THREADS_MAX =
        "alluxio.underfs.s3.admin.threads.max";
    public static final String UNDERFS_S3_DISABLE_DNS_BUCKETS =
        "alluxio.underfs.s3.disable.dns.buckets";
    public static final String UNDERFS_S3_ENDPOINT = "alluxio.underfs.s3.endpoint";
    public static final String UNDERFS_S3_ENDPOINT_HTTPS_PORT =
        "alluxio.underfs.s3.endpoint.https.port";
    public static final String UNDERFS_S3_ENDPOINT_HTTP_PORT =
        "alluxio.underfs.s3.endpoint.http.port";
    public static final String UNDERFS_S3_PROXY_HOST = "alluxio.underfs.s3.proxy.host";
    public static final String UNDERFS_S3_PROXY_HTTPS_ONLY = "alluxio.underfs.s3.proxy.https.only";
    public static final String UNDERFS_S3_PROXY_PORT = "alluxio.underfs.s3.proxy.port";
    public static final String UNDERFS_S3_THREADS_MAX = "alluxio.underfs.s3.threads.max";
    public static final String UNDERFS_S3_UPLOAD_THREADS_MAX =
        "alluxio.underfs.s3.upload.threads.max";
    public static final String USER_BLOCK_MASTER_CLIENT_THREADS =
        "alluxio.user.block.master.client.threads";
    public static final String USER_BLOCK_REMOTE_READER = "alluxio.user.block.remote.reader.class";
    public static final String USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.block.remote.read.buffer.size.bytes";
    public static final String USER_BLOCK_REMOTE_WRITER = "alluxio.user.block.remote.writer.class";
    public static final String USER_BLOCK_SIZE_BYTES_DEFAULT =
        "alluxio.user.block.size.bytes.default";
    public static final String USER_BLOCK_WORKER_CLIENT_THREADS =
        "alluxio.user.block.worker.client.threads";
    public static final String USER_FAILED_SPACE_REQUEST_LIMITS =
        "alluxio.user.failed.space.request.limits";
    public static final String USER_FILE_BUFFER_BYTES = "alluxio.user.file.buffer.bytes";
    public static final String USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
        "alluxio.user.file.cache.partially.read.block";
    public static final String USER_FILE_MASTER_CLIENT_THREADS =
        "alluxio.user.file.master.client.threads";
    public static final String USER_FILE_READ_TYPE_DEFAULT = "alluxio.user.file.readtype.default";
    public static final String USER_FILE_SEEK_BUFFER_SIZE_BYTES =
        "alluxio.user.file.seek.buffer.size.bytes";
    public static final String USER_FILE_WAITCOMPLETED_POLL_MS =
        "alluxio.user.file.waitcompleted.poll.ms";
    public static final String USER_FILE_WORKER_CLIENT_THREADS =
        "alluxio.user.file.worker.client.threads";
    public static final String USER_FILE_WRITE_LOCATION_POLICY =
        "alluxio.user.file.write.location.policy.class";
    public static final String USER_FILE_WRITE_TYPE_DEFAULT = "alluxio.user.file.writetype.default";
    public static final String USER_HEARTBEAT_INTERVAL_MS = "alluxio.user.heartbeat.interval.ms";
    public static final String USER_LINEAGE_ENABLED = "alluxio.user.lineage.enabled";
    public static final String USER_LINEAGE_MASTER_CLIENT_THREADS =
        "alluxio.user.lineage.master.client.threads";
    public static final String USER_NETWORK_NETTY_CHANNEL = "alluxio.user.network.netty.channel";
    public static final String USER_NETWORK_NETTY_TIMEOUT_MS =
        "alluxio.user.network.netty.timeout.ms";
    public static final String USER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.user.network.netty.worker.threads";
    public static final String USER_UFS_DELEGATION_ENABLED = "alluxio.user.ufs.delegation.enabled";
    public static final String USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.read.buffer.size.bytes";
    public static final String USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.write.buffer.size.bytes";
    public static final String VERSION = "alluxio.version";
    public static final String WEB_RESOURCES = "alluxio.web.resources";
    public static final String WEB_THREADS = "alluxio.web.threads";
    public static final String WORKER_ALLOCATOR_CLASS = "alluxio.worker.allocator.class";
    public static final String WORKER_BIND_HOST = "alluxio.worker.bind.host";
    public static final String WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.block.heartbeat.interval.ms";
    public static final String WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
        "alluxio.worker.block.heartbeat.timeout.ms";
    public static final String WORKER_DATA_BIND_HOST = "alluxio.worker.data.bind.host";
    public static final String WORKER_DATA_FOLDER = "alluxio.worker.data.folder";
    public static final String WORKER_DATA_HOSTNAME = "alluxio.worker.data.hostname";
    public static final String WORKER_DATA_PORT = "alluxio.worker.data.port";
    public static final String WORKER_DATA_SERVER_CLASS = "alluxio.worker.data.server.class";
    public static final String WORKER_DATA_TMP_FOLDER = "alluxio.worker.data.folder.tmp";
    public static final String WORKER_DATA_TMP_SUBDIR_MAX = "alluxio.worker.data.tmp.subdir.max";
    public static final String WORKER_EVICTOR_CLASS = "alluxio.worker.evictor.class";
    public static final String WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
        "alluxio.worker.evictor.lrfu.attenuation.factor";
    public static final String WORKER_EVICTOR_LRFU_STEP_FACTOR =
        "alluxio.worker.evictor.lrfu.step.factor";
    public static final String WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.filesystem.heartbeat.interval.ms";
    public static final String WORKER_FILE_PERSIST_POOL_SIZE =
        "alluxio.worker.file.persist.pool.size";
    public static final String WORKER_FILE_PERSIST_RATE_LIMIT =
        "alluxio.worker.file.persist.rate.limit";
    public static final String WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
        "alluxio.worker.file.persist.rate.limit.enabled";
    public static final String WORKER_HOSTNAME = "alluxio.worker.hostname";
    public static final String WORKER_KEYTAB_FILE = "alluxio.worker.keytab.file";
    public static final String WORKER_MEMORY_SIZE = "alluxio.worker.memory.size";
    public static final String WORKER_NETWORK_NETTY_BACKLOG =
        "alluxio.worker.network.netty.backlog";
    public static final String WORKER_NETWORK_NETTY_BOSS_THREADS =
        "alluxio.worker.network.netty.boss.threads";
    public static final String WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
        "alluxio.worker.network.netty.buffer.receive";
    public static final String WORKER_NETWORK_NETTY_BUFFER_SEND =
        "alluxio.worker.network.netty.buffer.send";
    public static final String WORKER_NETWORK_NETTY_CHANNEL =
        "alluxio.worker.network.netty.channel";
    public static final String WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
        "alluxio.worker.network.netty.file.transfer";
    public static final String WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
        "alluxio.worker.network.netty.shutdown.quiet.period";
    public static final String WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
        "alluxio.worker.network.netty.shutdown.timeout";
    public static final String WORKER_NETWORK_NETTY_WATERMARK_HIGH =
        "alluxio.worker.network.netty.watermark.high";
    public static final String WORKER_NETWORK_NETTY_WATERMARK_LOW =
        "alluxio.worker.network.netty.watermark.low";
    public static final String WORKER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.worker.network.netty.worker.threads";
    public static final String WORKER_PRINCIPAL = "alluxio.worker.principal";
    public static final String WORKER_RPC_PORT = "alluxio.worker.port";
    public static final String WORKER_SESSION_TIMEOUT_MS = "alluxio.worker.session.timeout.ms";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCKS =
        "alluxio.worker.tieredstore.block.locks";
    public static final String WORKER_TIERED_STORE_LEVEL0_ALIAS =
        "alluxio.worker.tieredstore.level0.alias";
    public static final String WORKER_TIERED_STORE_LEVEL0_DIRS_PATH =
        "alluxio.worker.tieredstore.level0.dirs.path";
    public static final String WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
        "alluxio.worker.tieredstore.level0.dirs.quota";
    public static final String WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
        "alluxio.worker.tieredstore.level0.reserved.ratio";
    public static final String WORKER_TIERED_STORE_LEVEL1_ALIAS =
        "alluxio.worker.tieredstore.level1.alias";
    public static final String WORKER_TIERED_STORE_LEVEL1_DIRS_PATH =
        "alluxio.worker.tieredstore.level1.dirs.path";
    public static final String WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
        "alluxio.worker.tieredstore.level1.dirs.quota";
    public static final String WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
        "alluxio.worker.tieredstore.level1.reserved.ratio";
    public static final String WORKER_TIERED_STORE_LEVEL2_ALIAS =
        "alluxio.worker.tieredstore.level2.alias";
    public static final String WORKER_TIERED_STORE_LEVEL2_DIRS_PATH =
        "alluxio.worker.tieredstore.level2.dirs.path";
    public static final String WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
        "alluxio.worker.tieredstore.level2.dirs.quota";
    public static final String WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
        "alluxio.worker.tieredstore.level2.reserved.ratio";
    public static final String WORKER_TIERED_STORE_LEVELS = "alluxio.worker.tieredstore.levels";
    public static final String WORKER_TIERED_STORE_RESERVER_ENABLED =
        "alluxio.worker.tieredstore.reserver.enabled";
    public static final String WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
        "alluxio.worker.tieredstore.reserver.interval.ms";
    public static final String WORKER_WEB_BIND_HOST = "alluxio.worker.web.bind.host";
    public static final String WORKER_WEB_HOSTNAME = "alluxio.worker.web.hostname";
    public static final String WORKER_WEB_PORT = "alluxio.worker.web.port";
    public static final String WORKER_WORKER_BLOCK_THREADS_MAX = "alluxio.worker.block.threads.max";
    public static final String WORKER_WORKER_BLOCK_THREADS_MIN = "alluxio.worker.block.threads.min";
    public static final String ZOOKEEPER_ADDRESS = "alluxio.zookeeper.address";
    public static final String ZOOKEEPER_ELECTION_PATH = "alluxio.zookeeper.election.path";
    public static final String ZOOKEEPER_ENABLED = "alluxio.zookeeper.enabled";
    public static final String ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
        "alluxio.zookeeper.leader.inquiry.retry";
    public static final String ZOOKEEPER_LEADER_PATH = "alluxio.zookeeper.leader.path";
  }

  /** A map from a property key's string name to the key. */
  private static final Map<String, PropertyKey> KEYS_MAP = initKeysMap();

  private final String mString;

  /**
   * @param keyStr string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String keyStr) {
    return KEYS_MAP.containsKey(keyStr);
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param keyStr string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String keyStr) {
    PropertyKey key = KEYS_MAP.get(keyStr);
    if (key == null) {
      throw new IllegalArgumentException("Invalid property key " + keyStr);
    }
    return key;
  }

  /**
   * @return a Map from property key name to the key
   */
  private static Map<String, PropertyKey> initKeysMap() {
    Map<String, PropertyKey> map = new HashMap<>();
    for (PropertyKey key : PropertyKey.values()) {
      map.put(key.toString(), key);
    }
    return map;
  }

  /**
   * Constructs a configuration property.
   *
   * @param property String of this property
   */
  PropertyKey(String property) {
    mString = property;
  }

  @Override
  public String toString() {
    return mString;
  }
}
