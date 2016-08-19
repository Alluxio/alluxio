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
  DEBUG(Name.DEBUG, false),
  HOME(Name.HOME, "/mnt/alluxio_default_home"),
  INTEGRATION_MASTER_RESOURCE_CPU(Name.INTEGRATION_MASTER_RESOURCE_CPU, 1),
  INTEGRATION_MASTER_RESOURCE_MEM(Name.INTEGRATION_MASTER_RESOURCE_MEM, "1024MB"),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NAME(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME,
      "AlluxioMaster"),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT, 1),
  INTEGRATION_MESOS_ALLUXIO_WORKER_NAME(Name.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME,
      "AlluxioWorker"),
  INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH(Name.INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH,
      "https://s3.amazonaws.com/alluxio-mesos"),
  INTEGRATION_MESOS_JRE_PATH(Name.INTEGRATION_MESOS_JRE_PATH, "jre1.7.0_79"),
  INTEGRATION_MESOS_JRE_URL(Name.INTEGRATION_MESOS_JRE_URL,
      "https://s3.amazonaws.com/alluxio-mesos/jre-7u79-linux-x64.tar.gz"),
  INTEGRATION_MESOS_PRINCIPAL(Name.INTEGRATION_MESOS_PRINCIPAL, "alluxio"),
  INTEGRATION_MESOS_ROLE(Name.INTEGRATION_MESOS_ROLE, "*"),
  INTEGRATION_MESOS_SECRET(Name.INTEGRATION_MESOS_SECRET, null),
  INTEGRATION_MESOS_USER(Name.INTEGRATION_MESOS_USER, "root"),
  INTEGRATION_WORKER_RESOURCE_CPU(Name.INTEGRATION_WORKER_RESOURCE_CPU, 1),
  INTEGRATION_WORKER_RESOURCE_MEM(Name.INTEGRATION_WORKER_RESOURCE_MEM, "1024MB"),
  INTEGRATION_YARN_WORKERS_PER_HOST_MAX(Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX, 1),
  KEY_VALUE_ENABLED(Name.KEY_VALUE_ENABLED, false),
  KEY_VALUE_PARTITION_SIZE_BYTES_MAX(Name.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, "512MB"),
  LOGGER_TYPE(Name.LOGGER_TYPE, "Console"),
  LOGS_DIR(Name.LOGS_DIR, "${alluxio.home}/logs"),
  METRICS_CONF_FILE(Name.METRICS_CONF_FILE, "${alluxio.home}/conf/metrics.properties"),
  NETWORK_HOST_RESOLUTION_TIMEOUT_MS(Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, 5000),
  NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX(Name.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX, "16MB"),
  SITE_CONF_DIR(Name.SITE_CONF_DIR, "${user.home}/.alluxio/,/etc/alluxio/"),
  TEST_MODE(Name.TEST_MODE, false),
  VERSION(Name.VERSION, ProjectConstants.VERSION),
  WEB_RESOURCES(Name.WEB_RESOURCES, "${alluxio.home}/core/server/src/main/webapp"),
  WEB_THREADS(Name.WEB_THREADS, 1),
  ZOOKEEPER_ADDRESS(Name.ZOOKEEPER_ADDRESS, null),
  ZOOKEEPER_ELECTION_PATH(Name.ZOOKEEPER_ELECTION_PATH, "/election"),
  ZOOKEEPER_ENABLED(Name.ZOOKEEPER_ENABLED, false),
  ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT(Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT, 10),
  ZOOKEEPER_LEADER_PATH(Name.ZOOKEEPER_LEADER_PATH, "/leader"),

  //
  // UFS related properties
  //
  UNDERFS_ADDRESS(Name.UNDERFS_ADDRESS, "${alluxio.home}/underFSStorage"),
  UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING(Name.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING, ""),
  UNDERFS_GLUSTERFS_IMPL(Name.UNDERFS_GLUSTERFS_IMPL,
      "org.apache.hadoop.fs.glusterfs.GlusterFileSystem"),
  UNDERFS_GLUSTERFS_MOUNTS(Name.UNDERFS_GLUSTERFS_MOUNTS, null),
  UNDERFS_GLUSTERFS_MR_DIR(Name.UNDERFS_GLUSTERFS_MR_DIR, "glusterfs:///mapred/system"),
  UNDERFS_GLUSTERFS_VOLUMES(Name.UNDERFS_GLUSTERFS_VOLUMES, null),
  UNDERFS_HDFS_CONFIGURATION(Name.UNDERFS_HDFS_CONFIGURATION, "${alluxio.home}/conf/core-site.xml"),
  UNDERFS_HDFS_IMPL(Name.UNDERFS_HDFS_IMPL, "org.apache.hadoop.hdfs.DistributedFileSystem"),
  UNDERFS_HDFS_PREFIXES(Name.UNDERFS_HDFS_PREFIXES, "hdfs://,glusterfs:///"),
  UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY(Name.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY,
      false),
  UNDERFS_OSS_CONNECT_MAX(Name.UNDERFS_OSS_CONNECT_MAX, 1024),
  UNDERFS_OSS_CONNECT_TIMEOUT(Name.UNDERFS_OSS_CONNECT_TIMEOUT, 50000),
  UNDERFS_OSS_CONNECT_TTL(Name.UNDERFS_OSS_CONNECT_TTL, -1),
  UNDERFS_OSS_SOCKET_TIMEOUT(Name.UNDERFS_OSS_SOCKET_TIMEOUT, 50000),
  UNDERFS_S3_ADMIN_THREADS_MAX(Name.UNDERFS_S3_ADMIN_THREADS_MAX, 20),
  UNDERFS_S3_DISABLE_DNS_BUCKETS(Name.UNDERFS_S3_DISABLE_DNS_BUCKETS, null),
  UNDERFS_S3_ENDPOINT(Name.UNDERFS_S3_ENDPOINT, null),
  UNDERFS_S3_ENDPOINT_HTTP_PORT(Name.UNDERFS_S3_ENDPOINT_HTTP_PORT, null),
  UNDERFS_S3_ENDPOINT_HTTPS_PORT(Name.UNDERFS_S3_ENDPOINT_HTTPS_PORT, null),
  UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING(Name.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING, ""),
  UNDERFS_S3_PROXY_HOST(Name.UNDERFS_S3_PROXY_HOST, null),
  UNDERFS_S3_PROXY_HTTPS_ONLY(Name.UNDERFS_S3_PROXY_HTTPS_ONLY, true),
  UNDERFS_S3_PROXY_PORT(Name.UNDERFS_S3_PROXY_PORT, null),
  UNDERFS_S3_THREADS_MAX(Name.UNDERFS_S3_THREADS_MAX, 22),
  UNDERFS_S3_UPLOAD_THREADS_MAX(Name.UNDERFS_S3_UPLOAD_THREADS_MAX, 2),
  UNDERFS_S3A_SECURE_HTTP_ENABLED(Name.UNDERFS_S3A_SECURE_HTTP_ENABLED, false),
  UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED(Name.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED,
      false),
  UNDERFS_S3A_SOCKET_TIMEOUT_MS(Name.UNDERFS_S3A_SOCKET_TIMEOUT_MS, 50000),

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  GCS_ACCESS_KEY(Name.GCS_ACCESS_KEY, null),
  GCS_SECRET_KEY(Name.GCS_SECRET_KEY, null),
  OSS_ACCESS_KEY(Name.OSS_ACCESS_KEY, null),
  OSS_ENDPOINT_KEY(Name.OSS_ENDPOINT_KEY, null),
  OSS_SECRET_KEY(Name.OSS_SECRET_KEY, null),
  S3A_ACCESS_KEY(Name.S3A_ACCESS_KEY, null),
  S3A_SECRET_KEY(Name.S3A_SECRET_KEY, null),
  S3N_ACCESS_KEY(Name.S3N_ACCESS_KEY, null),
  S3N_SECRET_KEY(Name.S3N_SECRET_KEY, null),
  SWIFT_API_KEY(Name.SWIFT_API_KEY, null),
  SWIFT_AUTH_METHOD_KEY(Name.SWIFT_AUTH_METHOD_KEY, null),
  SWIFT_AUTH_PORT_KEY(Name.SWIFT_AUTH_PORT_KEY, null),
  SWIFT_AUTH_URL_KEY(Name.SWIFT_AUTH_URL_KEY, null),
  SWIFT_PASSWORD_KEY(Name.SWIFT_PASSWORD_KEY, null),
  SWIFT_SIMULATION(Name.SWIFT_SIMULATION, null),
  SWIFT_TENANT_KEY(Name.SWIFT_TENANT_KEY, null),
  SWIFT_USE_PUBLIC_URI_KEY(Name.SWIFT_USE_PUBLIC_URI_KEY, null),
  SWIFT_USER_KEY(Name.SWIFT_USER_KEY, null),

  //
  // Master related properties
  //
  MASTER_ADDRESS(Name.MASTER_ADDRESS, null),
  MASTER_BIND_HOST(Name.MASTER_BIND_HOST, "0.0.0.0"),
  MASTER_FILE_ASYNC_PERSIST_HANDLER(Name.MASTER_FILE_ASYNC_PERSIST_HANDLER,
      "alluxio.master.file.async.DefaultAsyncPersistHandler"),
  MASTER_FORMAT_FILE_PREFIX(Name.MASTER_FORMAT_FILE_PREFIX, "_format_"),
  MASTER_HEARTBEAT_INTERVAL_MS(Name.MASTER_HEARTBEAT_INTERVAL_MS, 1000),
  MASTER_HOSTNAME(Name.MASTER_HOSTNAME, null),
  MASTER_JOURNAL_FLUSH_BATCH_TIME_MS(Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, 5),
  MASTER_JOURNAL_FOLDER(Name.MASTER_JOURNAL_FOLDER, "${alluxio.home}/journal/"),
  MASTER_JOURNAL_FORMATTER_CLASS(Name.MASTER_JOURNAL_FORMATTER_CLASS,
      "alluxio.master.journal.ProtoBufJournalFormatter"),
  MASTER_JOURNAL_LOG_SIZE_BYTES_MAX(Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "10MB"),
  MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS(
      Name.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 5000),
  MASTER_JOURNAL_TAILER_SLEEP_TIME_MS(Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, 1000),
  MASTER_KEYTAB_KEY_FILE(Name.MASTER_KEYTAB_KEY_FILE, null),
  MASTER_LINEAGE_CHECKPOINT_CLASS(Name.MASTER_LINEAGE_CHECKPOINT_CLASS,
      "alluxio.master.lineage.checkpoint.CheckpointLatestPlanner"),
  MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS(Name.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS, 300000),
  MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS(Name.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS, 300000),
  MASTER_LINEAGE_RECOMPUTE_LOG_PATH(Name.MASTER_LINEAGE_RECOMPUTE_LOG_PATH,
      "${alluxio.home}/logs/recompute.log"),
  MASTER_PRINCIPAL(Name.MASTER_PRINCIPAL, null),
  MASTER_RETRY(Name.MASTER_RETRY, 29),
  MASTER_RPC_PORT(Name.MASTER_RPC_PORT, 19998),
  MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS, "MEM"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS, "SSD"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS, "HDD"),
  MASTER_TIERED_STORE_GLOBAL_LEVELS(Name.MASTER_TIERED_STORE_GLOBAL_LEVELS, 3),
  MASTER_TTL_CHECKER_INTERVAL_MS(Name.MASTER_TTL_CHECKER_INTERVAL_MS, 3600000),
  MASTER_WEB_BIND_HOST(Name.MASTER_WEB_BIND_HOST, "0.0.0.0"),
  MASTER_WEB_HOSTNAME(Name.MASTER_WEB_HOSTNAME, null),
  MASTER_WEB_PORT(Name.MASTER_WEB_PORT, 19999),
  MASTER_WHITELIST(Name.MASTER_WHITELIST, "/"),
  MASTER_WORKER_THREADS_MAX(Name.MASTER_WORKER_THREADS_MAX, 2048),
  MASTER_WORKER_THREADS_MIN(Name.MASTER_WORKER_THREADS_MIN, 512),
  MASTER_WORKER_TIMEOUT_MS(Name.MASTER_WORKER_TIMEOUT_MS, 300000),

  //
  // Worker related properties
  //
  WORKER_ALLOCATOR_CLASS(Name.WORKER_ALLOCATOR_CLASS,
      "alluxio.worker.block.allocator.MaxFreeAllocator"),
  WORKER_BIND_HOST(Name.WORKER_BIND_HOST, "0.0.0.0"),
  WORKER_BLOCK_HEARTBEAT_INTERVAL_MS(Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, 1000),
  WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS(Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, 10000),
  WORKER_BLOCK_THREADS_MAX(Name.WORKER_BLOCK_THREADS_MAX, 2048),
  WORKER_BLOCK_THREADS_MIN(Name.WORKER_BLOCK_THREADS_MIN, 256),
  WORKER_DATA_BIND_HOST(Name.WORKER_DATA_BIND_HOST, "0.0.0.0"),
  WORKER_DATA_FOLDER(Name.WORKER_DATA_FOLDER, "/alluxioworker/"),
  WORKER_DATA_HOSTNAME(Name.WORKER_DATA_HOSTNAME, null),
  WORKER_DATA_PORT(Name.WORKER_DATA_PORT, 29999),
  WORKER_DATA_SERVER_CLASS(Name.WORKER_DATA_SERVER_CLASS, "alluxio.worker.netty.NettyDataServer"),
  WORKER_DATA_TMP_FOLDER(Name.WORKER_DATA_TMP_FOLDER, ".tmp_blocks"),
  WORKER_DATA_TMP_SUBDIR_MAX(Name.WORKER_DATA_TMP_SUBDIR_MAX, 1024),
  WORKER_EVICTOR_CLASS(Name.WORKER_EVICTOR_CLASS, "alluxio.worker.block.evictor.LRUEvictor"),
  WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR(Name.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR, 2.0),
  WORKER_EVICTOR_LRFU_STEP_FACTOR(Name.WORKER_EVICTOR_LRFU_STEP_FACTOR, 0.25),
  WORKER_FILE_PERSIST_POOL_SIZE(Name.WORKER_FILE_PERSIST_POOL_SIZE, 64),
  WORKER_FILE_PERSIST_RATE_LIMIT(Name.WORKER_FILE_PERSIST_RATE_LIMIT, "2GB"),
  WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED(Name.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED, false),
  WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS(Name.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS, 1000),
  WORKER_HOSTNAME(Name.WORKER_HOSTNAME, null),
  WORKER_KEYTAB_FILE(Name.WORKER_KEYTAB_FILE, null),
  WORKER_MEMORY_SIZE(Name.WORKER_MEMORY_SIZE, "1GB"),
  WORKER_NETWORK_NETTY_BACKLOG(Name.WORKER_NETWORK_NETTY_BACKLOG, null),
  WORKER_NETWORK_NETTY_BOSS_THREADS(Name.WORKER_NETWORK_NETTY_BOSS_THREADS, 1),
  WORKER_NETWORK_NETTY_BUFFER_RECEIVE(Name.WORKER_NETWORK_NETTY_BUFFER_RECEIVE, null),
  WORKER_NETWORK_NETTY_BUFFER_SEND(Name.WORKER_NETWORK_NETTY_BUFFER_SEND, null),
  WORKER_NETWORK_NETTY_CHANNEL(Name.WORKER_NETWORK_NETTY_CHANNEL, null),
  WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE(Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "MAPPED"),
  WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD(Name.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, 2),
  WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT(Name.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, 15),
  WORKER_NETWORK_NETTY_WATERMARK_HIGH(Name.WORKER_NETWORK_NETTY_WATERMARK_HIGH, "32KB"),
  WORKER_NETWORK_NETTY_WATERMARK_LOW(Name.WORKER_NETWORK_NETTY_WATERMARK_LOW, "8KB"),
  WORKER_NETWORK_NETTY_WORKER_THREADS(Name.WORKER_NETWORK_NETTY_WORKER_THREADS, 0),
  WORKER_PRINCIPAL(Name.WORKER_PRINCIPAL, null),
  WORKER_RPC_PORT(Name.WORKER_RPC_PORT, 29998),
  WORKER_SESSION_TIMEOUT_MS(Name.WORKER_SESSION_TIMEOUT_MS, 10000),
  WORKER_TIERED_STORE_BLOCK_LOCKS(Name.WORKER_TIERED_STORE_BLOCK_LOCKS, 1000),
  WORKER_TIERED_STORE_LEVEL0_ALIAS(Name.WORKER_TIERED_STORE_LEVEL0_ALIAS, "MEM"),
  WORKER_TIERED_STORE_LEVEL0_DIRS_PATH(Name.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, "/mnt/ramdisk"),
  WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA(Name.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
      "${alluxio.worker.memory.size}"),
  WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO(Name.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO, "0.1"),
  WORKER_TIERED_STORE_LEVEL1_ALIAS(Name.WORKER_TIERED_STORE_LEVEL1_ALIAS, null),
  WORKER_TIERED_STORE_LEVEL1_DIRS_PATH(Name.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, null),
  WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA(Name.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, null),
  WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO(Name.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO, null),
  WORKER_TIERED_STORE_LEVEL2_ALIAS(Name.WORKER_TIERED_STORE_LEVEL2_ALIAS, null),
  WORKER_TIERED_STORE_LEVEL2_DIRS_PATH(Name.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH, null),
  WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA(Name.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA, null),
  WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO(Name.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO, null),
  WORKER_TIERED_STORE_LEVELS(Name.WORKER_TIERED_STORE_LEVELS, 1),
  WORKER_TIERED_STORE_RESERVER_ENABLED(Name.WORKER_TIERED_STORE_RESERVER_ENABLED, false),
  WORKER_TIERED_STORE_RESERVER_INTERVAL_MS(Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, 1000),
  WORKER_WEB_BIND_HOST(Name.WORKER_WEB_BIND_HOST, "0.0.0.0"),
  WORKER_WEB_HOSTNAME(Name.WORKER_WEB_HOSTNAME, null),
  WORKER_WEB_PORT(Name.WORKER_WEB_PORT, 30000),

  //
  // User related properties
  //
  USER_BLOCK_MASTER_CLIENT_THREADS(Name.USER_BLOCK_MASTER_CLIENT_THREADS, 10),
  USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES(Name.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "8MB"),
  USER_BLOCK_REMOTE_READER_CLASS(Name.USER_BLOCK_REMOTE_READER_CLASS,
      "alluxio.client.netty.NettyRemoteBlockReader"),
  USER_BLOCK_REMOTE_WRITER_CLASS(Name.USER_BLOCK_REMOTE_WRITER_CLASS,
      "alluxio.client.netty.NettyRemoteBlockWriter"),
  USER_BLOCK_SIZE_BYTES_DEFAULT(Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "512MB"),
  USER_BLOCK_WORKER_CLIENT_THREADS(Name.USER_BLOCK_WORKER_CLIENT_THREADS, 128),
  USER_FAILED_SPACE_REQUEST_LIMITS(Name.USER_FAILED_SPACE_REQUEST_LIMITS, 3),
  USER_FILE_BUFFER_BYTES(Name.USER_FILE_BUFFER_BYTES, "1MB"),
  USER_FILE_CACHE_PARTIALLY_READ_BLOCK(Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, true),
  USER_FILE_MASTER_CLIENT_THREADS(Name.USER_FILE_MASTER_CLIENT_THREADS, 10),
  USER_FILE_READ_TYPE_DEFAULT(Name.USER_FILE_READ_TYPE_DEFAULT, "CACHE_PROMOTE"),
  USER_FILE_SEEK_BUFFER_SIZE_BYTES(Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES, "1MB"),
  USER_FILE_WAITCOMPLETED_POLL_MS(Name.USER_FILE_WAITCOMPLETED_POLL_MS, 1000),
  USER_FILE_WORKER_CLIENT_THREADS(Name.USER_FILE_WORKER_CLIENT_THREADS, 10),
  USER_FILE_WRITE_LOCATION_POLICY(Name.USER_FILE_WRITE_LOCATION_POLICY,
      "alluxio.client.file.policy.LocalFirstPolicy"),
  USER_FILE_WRITE_TYPE_DEFAULT(Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE"),
  USER_HEARTBEAT_INTERVAL_MS(Name.USER_HEARTBEAT_INTERVAL_MS, 1000),
  USER_LINEAGE_ENABLED(Name.USER_LINEAGE_ENABLED, false),
  USER_LINEAGE_MASTER_CLIENT_THREADS(Name.USER_LINEAGE_MASTER_CLIENT_THREADS, 10),
  USER_NETWORK_NETTY_CHANNEL(Name.USER_NETWORK_NETTY_CHANNEL, null),
  USER_NETWORK_NETTY_TIMEOUT_MS(Name.USER_NETWORK_NETTY_TIMEOUT_MS, 30000),
  USER_NETWORK_NETTY_WORKER_THREADS(Name.USER_NETWORK_NETTY_WORKER_THREADS, 0),
  USER_UFS_DELEGATION_ENABLED(Name.USER_UFS_DELEGATION_ENABLED, false),
  USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES(Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES,
      "8MB"),
  USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES(Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES,
      "2MB"),

  //
  // FUSE integration related properties
  //
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  FUSE_CACHED_PATHS_MAX(Name.FUSE_CACHED_PATHS_MAX, 500),
  /** Have the fuse process log every FS request. */
  FUSE_DEBUG_ENABLED(Name.FUSE_DEBUG_ENABLED, false),

  /** FUSE file system name. */
  FUSE_FS_NAME(Name.FUSE_FS_NAME, "alluxio-fuse"),
  FUSE_FS_ROOT(Name.FUSE_FS_ROOT, "/"),
  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  FUSE_MAXWRITE_BYTES(Name.FUSE_MAXWRITE_BYTES, 131072),
  FUSE_MOUNT_DEFAULT(Name.FUSE_MOUNT_DEFAULT, "/mnt/alluxio"),

  //
  // Security related properties
  //
  SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS(Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
      null),
  SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS(Name.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS,
      "600000"),
  SECURITY_AUTHENTICATION_TYPE(Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"),
  SECURITY_AUTHORIZATION_PERMISSION_ENABLED(Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false),
  SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP(Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP,
      "supergroup"),
  SECURITY_AUTHORIZATION_PERMISSION_UMASK(Name.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "022"),
  SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS(Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS, "60000"),
  SECURITY_GROUP_MAPPING_CLASS(Name.SECURITY_GROUP_MAPPING_CLASS,
      "alluxio.security.group.provider.ShellBasedUnixGroupsMapping"),
  SECURITY_LOGIN_USERNAME(Name.SECURITY_LOGIN_USERNAME, null),
  ;

  /**
   * A nested class to hold named string constants for their corresponding enum values.
   * Used for setting configuration in integration tests.
   */
  public static final class Name {
    public static final String DEBUG = "alluxio.debug";
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
    public static final String METRICS_CONF_FILE = "alluxio.metrics.conf.file";
    public static final String NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
        "alluxio.network.host.resolution.timeout.ms";
    public static final String NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
        "alluxio.network.thrift.frame.size.bytes.max";
    public static final String SITE_CONF_DIR = "alluxio.site.conf.dir";
    public static final String TEST_MODE = "alluxio.test.mode";
    public static final String VERSION = "alluxio.version";
    public static final String WEB_RESOURCES = "alluxio.web.resources";
    public static final String WEB_THREADS = "alluxio.web.threads";
    public static final String ZOOKEEPER_ADDRESS = "alluxio.zookeeper.address";
    public static final String ZOOKEEPER_ELECTION_PATH = "alluxio.zookeeper.election.path";
    public static final String ZOOKEEPER_ENABLED = "alluxio.zookeeper.enabled";
    public static final String ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
        "alluxio.zookeeper.leader.inquiry.retry";
    public static final String ZOOKEEPER_LEADER_PATH = "alluxio.zookeeper.leader.path";

    //
    // UFS related properties
    //
    public static final String UNDERFS_ADDRESS = "alluxio.underfs.address";
    public static final String UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
        "alluxio.underfs.gcs.owner.id.to.username.mapping";
    public static final String UNDERFS_GLUSTERFS_IMPL = "alluxio.underfs.glusterfs.impl";
    public static final String UNDERFS_GLUSTERFS_MOUNTS = "alluxio.underfs.glusterfs.mounts";
    public static final String UNDERFS_GLUSTERFS_MR_DIR =
        "alluxio.underfs.glusterfs.mapred.system.dir";
    public static final String UNDERFS_GLUSTERFS_VOLUMES = "alluxio.underfs.glusterfs.volumes";
    public static final String UNDERFS_HDFS_CONFIGURATION = "alluxio.underfs.hdfs.configuration";
    public static final String UNDERFS_HDFS_IMPL = "alluxio.underfs.hdfs.impl";
    public static final String UNDERFS_HDFS_PREFIXES = "alluxio.underfs.hdfs.prefixes";
    public static final String UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
        "alluxio.underfs.object.store.mount.shared.publicly";
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
    public static final String UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
        "alluxio.underfs.s3.owner.id.to.username.mapping";
    public static final String UNDERFS_S3_PROXY_HOST = "alluxio.underfs.s3.proxy.host";
    public static final String UNDERFS_S3_PROXY_HTTPS_ONLY = "alluxio.underfs.s3.proxy.https.only";
    public static final String UNDERFS_S3_PROXY_PORT = "alluxio.underfs.s3.proxy.port";
    public static final String UNDERFS_S3_THREADS_MAX = "alluxio.underfs.s3.threads.max";
    public static final String UNDERFS_S3_UPLOAD_THREADS_MAX =
        "alluxio.underfs.s3.upload.threads.max";

    //
    // UFS access control related properties
    //
    public static final String GCS_ACCESS_KEY = "fs.gcs.accessKeyId";
    public static final String GCS_SECRET_KEY = "fs.gcs.secretAccessKey";
    public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    public static final String OSS_ENDPOINT_KEY = "fs.oss.endpoint";
    public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    public static final String S3A_ACCESS_KEY = "aws.accessKeyId";
    public static final String S3A_SECRET_KEY = "aws.secretKey";
    public static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
    public static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";
    public static final String SWIFT_API_KEY = "fs.swift.apikey";
    public static final String SWIFT_AUTH_METHOD_KEY = "fs.swift.auth.method";
    public static final String SWIFT_AUTH_PORT_KEY = "fs.swift.auth.port";
    public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
    public static final String SWIFT_PASSWORD_KEY = "fs.swift.password";
    public static final String SWIFT_SIMULATION = "fs.swift.simulation";
    public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
    public static final String SWIFT_USER_KEY = "fs.swift.user";
    public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";

    //
    // Master related properties
    //
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

    //
    // Worker related properties
    //
    public static final String WORKER_ALLOCATOR_CLASS = "alluxio.worker.allocator.class";
    public static final String WORKER_BIND_HOST = "alluxio.worker.bind.host";
    public static final String WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.block.heartbeat.interval.ms";
    public static final String WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
        "alluxio.worker.block.heartbeat.timeout.ms";
    public static final String WORKER_BLOCK_THREADS_MAX = "alluxio.worker.block.threads.max";
    public static final String WORKER_BLOCK_THREADS_MIN = "alluxio.worker.block.threads.min";
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

    //
    // User related properties
    //
    public static final String USER_BLOCK_MASTER_CLIENT_THREADS =
        "alluxio.user.block.master.client.threads";
    public static final String USER_BLOCK_REMOTE_READER_CLASS =
        "alluxio.user.block.remote.reader.class";
    public static final String USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.block.remote.read.buffer.size.bytes";
    public static final String USER_BLOCK_REMOTE_WRITER_CLASS =
        "alluxio.user.block.remote.writer.class";
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

    //
    // FUSE integration related properties
    //
    public static final String FUSE_CACHED_PATHS_MAX = "alluxio.fuse.cached.paths.max";
    public static final String FUSE_DEBUG_ENABLED = "alluxio.fuse.debug.enabled";
    public static final String FUSE_FS_NAME = "alluxio.fuse.fs.name";
    public static final String FUSE_FS_ROOT = "alluxio.fuse.fs.root";
    public static final String FUSE_MAXWRITE_BYTES = "alluxio.fuse.maxwrite.bytes";
    public static final String FUSE_MOUNT_DEFAULT = "alluxio.fuse.mount.default";

    //
    // Security related properties
    //
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

    private Name() {} // prevent instantiation
  }

  /** A map from a property key's string name to the key. */
  private static final Map<String, PropertyKey> KEYS_MAP = initKeysMap();

  /** Property name. */
  private final String mName;

  /** Property name. */
  private final String mDefaultValue;

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
   * @param defaultValue Default value of this property in compile time if not null
   */
  PropertyKey(String property, Object defaultValue) {
    mName = property;
    if (defaultValue == null) {
      mDefaultValue = null;
    } else {
      mDefaultValue = defaultValue.toString();
    }
  }

  @Override
  public String toString() {
    return mName;
  }

  /**
   * @return the default value of a property key
   */
  public String getDefaultValue() {
    return mDefaultValue;
  }

}
