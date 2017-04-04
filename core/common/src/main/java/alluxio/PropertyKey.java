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

import com.google.common.base.Objects;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configurations properties constants. Please check and update Configuration-Settings.md file when
 * you change or add Alluxio configuration properties.
 */
@ThreadSafe
public final class PropertyKey {
  /** A map from default property key's string name to the key. */
  private static final Map<PropertyKey, Object> DEFAULT_VALUES;

  /** A map from default property key's string name to the key. */
  private static final Map<String, PropertyKey> DEFAULT_KEYS_MAP;

  static {
    DEFAULT_VALUES = new HashMap<>();
    DEFAULT_KEYS_MAP = new HashMap<>();
  }


  public static final PropertyKey CONF_DIR =
      defaultKey(Name.CONF_DIR, String.format("${%s}/conf", Name.HOME));
  public static final PropertyKey DEBUG = defaultKey(Name.DEBUG, false);
  public static final PropertyKey HOME = defaultKey(Name.HOME, "/opt/alluxio");
  public static final PropertyKey KEY_VALUE_ENABLED = defaultKey(Name.KEY_VALUE_ENABLED, false);
  public static final PropertyKey KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
      defaultKey(Name.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, "512MB");
  public static final PropertyKey LOGGER_TYPE = defaultKey(Name.LOGGER_TYPE, "Console");
  public static final PropertyKey LOGS_DIR =
      defaultKey(Name.LOGS_DIR, String.format("${%s}/logs", Name.WORK_DIR));
  public static final PropertyKey METRICS_CONF_FILE =
      defaultKey(Name.METRICS_CONF_FILE, String.format("${%s}/metrics.properties", Name.CONF_DIR));
  public static final PropertyKey NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      defaultKey(Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, 5000);
  public static final PropertyKey NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
      defaultKey(Name.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX, "16MB");
  public static final PropertyKey SITE_CONF_DIR =
      defaultKey(Name.SITE_CONF_DIR, "${user.home}/.alluxio/,/etc/alluxio/");
  public static final PropertyKey TEST_MODE = defaultKey(Name.TEST_MODE, false);
  public static final PropertyKey VERSION = defaultKey(Name.VERSION, ProjectConstants.VERSION);
  public static final PropertyKey WEB_RESOURCES = defaultKey(Name.WEB_RESOURCES,
      String.format("${%s}/core/server/common/src/main/webapp", Name.HOME));
  public static final PropertyKey WEB_THREADS = defaultKey(Name.WEB_THREADS, 1);
  public static final PropertyKey WORK_DIR =
      defaultKey(Name.WORK_DIR, String.format("${%s}", Name.HOME));
  public static final PropertyKey ZOOKEEPER_ADDRESS = defaultKey(Name.ZOOKEEPER_ADDRESS, null);
  public static final PropertyKey ZOOKEEPER_ELECTION_PATH =
      defaultKey(Name.ZOOKEEPER_ELECTION_PATH, "/election");
  public static final PropertyKey ZOOKEEPER_ENABLED = defaultKey(Name.ZOOKEEPER_ENABLED, false);
  public static final PropertyKey ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      defaultKey(Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT, 10);
  public static final PropertyKey ZOOKEEPER_LEADER_PATH =
      defaultKey(Name.ZOOKEEPER_LEADER_PATH, "/leader");

  //
  // UFS related properties
  //
  // Deprecated
  public static final PropertyKey UNDERFS_ADDRESS =
      defaultKey(Name.UNDERFS_ADDRESS, String.format("${%s}/underFSStorage", Name.WORK_DIR));
  public static final PropertyKey UNDERFS_ALLOW_SET_OWNER_FAILURE =
      defaultKey(Name.UNDERFS_ALLOW_SET_OWNER_FAILURE, false);
  public static final PropertyKey UNDERFS_LISTING_LENGTH =
      defaultKey(Name.UNDERFS_LISTING_LENGTH, 1000);
  public static final PropertyKey UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
      defaultKey(Name.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING, "");
  public static final PropertyKey UNDERFS_GLUSTERFS_IMPL =
      defaultKey(Name.UNDERFS_GLUSTERFS_IMPL, "org.apache.hadoop.fs.glusterfs.GlusterFileSystem");
  public static final PropertyKey UNDERFS_GLUSTERFS_MOUNTS =
      defaultKey(Name.UNDERFS_GLUSTERFS_MOUNTS, null);
  public static final PropertyKey UNDERFS_GLUSTERFS_MR_DIR =
      defaultKey(Name.UNDERFS_GLUSTERFS_MR_DIR, "glusterfs:///mapred/system");
  public static final PropertyKey UNDERFS_GLUSTERFS_VOLUMES =
      defaultKey(Name.UNDERFS_GLUSTERFS_VOLUMES, null);
  public static final PropertyKey UNDERFS_HDFS_CONFIGURATION =
      defaultKey(Name.UNDERFS_HDFS_CONFIGURATION,
          String.format("${%s}/core-site.xml", Name.CONF_DIR));
  public static final PropertyKey UNDERFS_HDFS_IMPL =
      defaultKey(Name.UNDERFS_HDFS_IMPL, "org.apache.hadoop.hdfs.DistributedFileSystem");
  public static final PropertyKey UNDERFS_HDFS_PREFIXES =
      defaultKey(Name.UNDERFS_HDFS_PREFIXES, "hdfs://,glusterfs:///,maprfs:///");
  public static final PropertyKey UNDERFS_HDFS_REMOTE = defaultKey(Name.UNDERFS_HDFS_REMOTE, false);
  public static final PropertyKey UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
      defaultKey(Name.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY, false);
  public static final PropertyKey UNDERFS_OSS_CONNECT_MAX =
      defaultKey(Name.UNDERFS_OSS_CONNECT_MAX, 1024);
  public static final PropertyKey UNDERFS_OSS_CONNECT_TIMEOUT =
      defaultKey(Name.UNDERFS_OSS_CONNECT_TIMEOUT, 50000);
  public static final PropertyKey UNDERFS_OSS_CONNECT_TTL =
      defaultKey(Name.UNDERFS_OSS_CONNECT_TTL, -1);
  public static final PropertyKey UNDERFS_OSS_SOCKET_TIMEOUT =
      defaultKey(Name.UNDERFS_OSS_SOCKET_TIMEOUT, 50000);
  public static final PropertyKey UNDERFS_S3_ADMIN_THREADS_MAX =
      defaultKey(Name.UNDERFS_S3_ADMIN_THREADS_MAX, 20);
  public static final PropertyKey UNDERFS_S3_DISABLE_DNS_BUCKETS =
      defaultKey(Name.UNDERFS_S3_DISABLE_DNS_BUCKETS, false);
  public static final PropertyKey UNDERFS_S3_ENDPOINT = defaultKey(Name.UNDERFS_S3_ENDPOINT, null);
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTP_PORT =
      defaultKey(Name.UNDERFS_S3_ENDPOINT_HTTP_PORT, null);
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTPS_PORT =
      defaultKey(Name.UNDERFS_S3_ENDPOINT_HTTPS_PORT, null);
  public static final PropertyKey UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
      defaultKey(Name.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING, "");
  public static final PropertyKey UNDERFS_S3_PROXY_HOST =
      defaultKey(Name.UNDERFS_S3_PROXY_HOST, null);
  public static final PropertyKey UNDERFS_S3_PROXY_HTTPS_ONLY =
      defaultKey(Name.UNDERFS_S3_PROXY_HTTPS_ONLY, true);
  public static final PropertyKey UNDERFS_S3_PROXY_PORT =
      defaultKey(Name.UNDERFS_S3_PROXY_PORT, null);
  public static final PropertyKey UNDERFS_S3_THREADS_MAX =
      defaultKey(Name.UNDERFS_S3_THREADS_MAX, 40);
  public static final PropertyKey UNDERFS_S3_UPLOAD_THREADS_MAX =
      defaultKey(Name.UNDERFS_S3_UPLOAD_THREADS_MAX, 20);
  public static final PropertyKey UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
      defaultKey(Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS, 60000);
  public static final PropertyKey UNDERFS_S3A_INHERIT_ACL =
      defaultKey(Name.UNDERFS_S3A_INHERIT_ACL, true);
  public static final PropertyKey UNDERFS_S3A_REQUEST_TIMEOUT =
      defaultKey(Name.UNDERFS_S3A_REQUEST_TIMEOUT_MS, 60000);
  public static final PropertyKey UNDERFS_S3A_SECURE_HTTP_ENABLED =
      defaultKey(Name.UNDERFS_S3A_SECURE_HTTP_ENABLED, false);
  public static final PropertyKey UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
      defaultKey(Name.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED, false);
  public static final PropertyKey UNDERFS_S3A_SOCKET_TIMEOUT_MS =
      defaultKey(Name.UNDERFS_S3A_SOCKET_TIMEOUT_MS, 50000);

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  public static final PropertyKey GCS_ACCESS_KEY = defaultKey(Name.GCS_ACCESS_KEY, null);
  public static final PropertyKey GCS_SECRET_KEY = defaultKey(Name.GCS_SECRET_KEY, null);
  public static final PropertyKey OSS_ACCESS_KEY = defaultKey(Name.OSS_ACCESS_KEY, null);
  public static final PropertyKey OSS_ENDPOINT_KEY = defaultKey(Name.OSS_ENDPOINT_KEY, null);
  public static final PropertyKey OSS_SECRET_KEY = defaultKey(Name.OSS_SECRET_KEY, null);
  public static final PropertyKey S3A_ACCESS_KEY = defaultKey(Name.S3A_ACCESS_KEY, null);
  public static final PropertyKey S3A_SECRET_KEY = defaultKey(Name.S3A_SECRET_KEY, null);
  public static final PropertyKey S3N_ACCESS_KEY = defaultKey(Name.S3N_ACCESS_KEY, null);
  public static final PropertyKey S3N_SECRET_KEY = defaultKey(Name.S3N_SECRET_KEY, null);
  public static final PropertyKey SWIFT_API_KEY = defaultKey(Name.SWIFT_API_KEY, null);
  public static final PropertyKey SWIFT_AUTH_METHOD_KEY =
      defaultKey(Name.SWIFT_AUTH_METHOD_KEY, null);
  public static final PropertyKey SWIFT_AUTH_URL_KEY = defaultKey(Name.SWIFT_AUTH_URL_KEY, null);
  public static final PropertyKey SWIFT_PASSWORD_KEY = defaultKey(Name.SWIFT_PASSWORD_KEY, null);
  public static final PropertyKey SWIFT_SIMULATION = defaultKey(Name.SWIFT_SIMULATION, null);
  public static final PropertyKey SWIFT_TENANT_KEY = defaultKey(Name.SWIFT_TENANT_KEY, null);
  public static final PropertyKey SWIFT_USE_PUBLIC_URI_KEY =
      defaultKey(Name.SWIFT_USE_PUBLIC_URI_KEY, null);
  public static final PropertyKey SWIFT_USER_KEY = defaultKey(Name.SWIFT_USER_KEY, null);
  public static final PropertyKey SWIFT_REGION_KEY = defaultKey(Name.SWIFT_REGION_KEY, null);

  //
  // Mount table related properties
  //
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_ALLUXIO =
      defaultKey(
          ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("root").toString(), "/");
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_OPTION =
      defaultKey(
          ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_OPTION.format("root").toString(),
          null);
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_READONLY =
      defaultKey(
          ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_READONLY.format("root").toString(),
          false);
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_SHARED =
      defaultKey(
          ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_SHARED.format("root").toString(),
          true);
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_UFS =
      defaultKey(
          ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_UFS.format("root").toString(),
          String.format("${%s}", Name.UNDERFS_ADDRESS));

  //
  // Master related properties
  //
  public static final PropertyKey MASTER_ADDRESS = defaultKey(Name.MASTER_ADDRESS, null);
  public static final PropertyKey MASTER_BIND_HOST = defaultKey(Name.MASTER_BIND_HOST, "0.0.0.0");
  public static final PropertyKey MASTER_CONNECTION_TIMEOUT_MS =
      defaultKey(Name.MASTER_CONNECTION_TIMEOUT_MS, 0);
  public static final PropertyKey MASTER_FILE_ASYNC_PERSIST_HANDLER =
      defaultKey(Name.MASTER_FILE_ASYNC_PERSIST_HANDLER,
          "alluxio.master.file.async.DefaultAsyncPersistHandler");
  public static final PropertyKey MASTER_FORMAT_FILE_PREFIX =
      defaultKey(Name.MASTER_FORMAT_FILE_PREFIX, "_format_");
  public static final PropertyKey MASTER_HEARTBEAT_INTERVAL_MS =
      defaultKey(Name.MASTER_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey MASTER_HOSTNAME = defaultKey(Name.MASTER_HOSTNAME, null);
  public static final PropertyKey MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
      defaultKey(Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, 5);
  public static final PropertyKey MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
      defaultKey(Name.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, 300000);
  public static final PropertyKey MASTER_JOURNAL_FOLDER =
      defaultKey(Name.MASTER_JOURNAL_FOLDER, String.format("${%s}/journal", Name.WORK_DIR));
  public static final PropertyKey MASTER_JOURNAL_FORMATTER_CLASS =
      defaultKey(Name.MASTER_JOURNAL_FORMATTER_CLASS,
          "alluxio.master.journal.ProtoBufJournalFormatter");
  public static final PropertyKey MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
      defaultKey(Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "10MB");
  public static final PropertyKey MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
      defaultKey(Name.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 5000);
  public static final PropertyKey MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      defaultKey(Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, 1000);
  public static final PropertyKey MASTER_KEYTAB_KEY_FILE =
      defaultKey(Name.MASTER_KEYTAB_KEY_FILE, null);
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_CLASS =
      defaultKey(Name.MASTER_LINEAGE_CHECKPOINT_CLASS,
          "alluxio.master.lineage.checkpoint.CheckpointLatestPlanner");
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS =
      defaultKey(Name.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS, 300000);
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS =
      defaultKey(Name.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS, 300000);
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_LOG_PATH =
      defaultKey(Name.MASTER_LINEAGE_RECOMPUTE_LOG_PATH,
          String.format("${%s}/recompute.log", Name.LOGS_DIR));
  public static final PropertyKey MASTER_PRINCIPAL = defaultKey(Name.MASTER_PRINCIPAL, null);
  // deprecated since version 1.4 and will be removed in version 2.0
  // use USER_RPC_RETRY_MAX_NUM_RETRY instead
  public static final PropertyKey MASTER_RETRY =
      defaultKey(Name.MASTER_RETRY, String.format("${%s}", Name.USER_RPC_RETRY_MAX_NUM_RETRY));
  public static final PropertyKey MASTER_RPC_PORT = defaultKey(Name.MASTER_RPC_PORT, 19998);
  public static final PropertyKey MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
      defaultKey(Name.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED, true);
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
      defaultKey(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS, "MEM");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
      defaultKey(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS, "SSD");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
      defaultKey(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS, "HDD");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVELS =
      defaultKey(Name.MASTER_TIERED_STORE_GLOBAL_LEVELS, 3);
  public static final PropertyKey MASTER_TTL_CHECKER_INTERVAL_MS =
      defaultKey(Name.MASTER_TTL_CHECKER_INTERVAL_MS, 3600000);
  public static final PropertyKey MASTER_WEB_BIND_HOST =
      defaultKey(Name.MASTER_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey MASTER_WEB_HOSTNAME = defaultKey(Name.MASTER_WEB_HOSTNAME, null);
  public static final PropertyKey MASTER_WEB_PORT = defaultKey(Name.MASTER_WEB_PORT, 19999);
  public static final PropertyKey MASTER_WHITELIST = defaultKey(Name.MASTER_WHITELIST, "/");
  public static final PropertyKey MASTER_WORKER_THREADS_MAX =
      defaultKey(Name.MASTER_WORKER_THREADS_MAX, 2048);
  public static final PropertyKey MASTER_WORKER_THREADS_MIN =
      defaultKey(Name.MASTER_WORKER_THREADS_MIN, 512);
  public static final PropertyKey MASTER_WORKER_TIMEOUT_MS =
      defaultKey(Name.MASTER_WORKER_TIMEOUT_MS, 300000);

  //
  // Worker related properties
  //
  public static final PropertyKey WORKER_ALLOCATOR_CLASS =
      defaultKey(Name.WORKER_ALLOCATOR_CLASS, "alluxio.worker.block.allocator.MaxFreeAllocator");
  public static final PropertyKey WORKER_BIND_HOST = defaultKey(Name.WORKER_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      defaultKey(Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      defaultKey(Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, 60000);
  public static final PropertyKey WORKER_BLOCK_THREADS_MAX =
      defaultKey(Name.WORKER_BLOCK_THREADS_MAX, 2048);
  public static final PropertyKey WORKER_BLOCK_THREADS_MIN =
      defaultKey(Name.WORKER_BLOCK_THREADS_MIN, 256);
  public static final PropertyKey WORKER_DATA_BIND_HOST =
      defaultKey(Name.WORKER_DATA_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_DATA_FOLDER =
      defaultKey(Name.WORKER_DATA_FOLDER, "/alluxioworker/");
  public static final PropertyKey WORKER_DATA_HOSTNAME =
      defaultKey(Name.WORKER_DATA_HOSTNAME, null);
  public static final PropertyKey WORKER_DATA_PORT = defaultKey(Name.WORKER_DATA_PORT, 29999);
  public static final PropertyKey WORKER_DATA_SERVER_CLASS =
      defaultKey(Name.WORKER_DATA_SERVER_CLASS, "alluxio.worker.netty.NettyDataServer");
  public static final PropertyKey WORKER_DATA_TMP_FOLDER =
      defaultKey(Name.WORKER_DATA_TMP_FOLDER, ".tmp_blocks");
  public static final PropertyKey WORKER_DATA_TMP_SUBDIR_MAX =
      defaultKey(Name.WORKER_DATA_TMP_SUBDIR_MAX, 1024);
  public static final PropertyKey WORKER_EVICTOR_CLASS =
      defaultKey(Name.WORKER_EVICTOR_CLASS, "alluxio.worker.block.evictor.LRUEvictor");
  public static final PropertyKey WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
      defaultKey(Name.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR, 2.0);
  public static final PropertyKey WORKER_EVICTOR_LRFU_STEP_FACTOR =
      defaultKey(Name.WORKER_EVICTOR_LRFU_STEP_FACTOR, 0.25);
  public static final PropertyKey WORKER_FILE_PERSIST_POOL_SIZE =
      defaultKey(Name.WORKER_FILE_PERSIST_POOL_SIZE, 64);
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT =
      defaultKey(Name.WORKER_FILE_PERSIST_RATE_LIMIT, "2GB");
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
      defaultKey(Name.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED, false);
  public static final PropertyKey WORKER_FILE_BUFFER_SIZE =
      defaultKey(Name.WORKER_FILE_BUFFER_SIZE, "1MB");
  public static final PropertyKey WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
      defaultKey(Name.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_HOSTNAME = defaultKey(Name.WORKER_HOSTNAME, null);
  public static final PropertyKey WORKER_KEYTAB_FILE = defaultKey(Name.WORKER_KEYTAB_FILE, null);
  public static final PropertyKey WORKER_MEMORY_SIZE = defaultKey(Name.WORKER_MEMORY_SIZE, "1GB");
  public static final PropertyKey WORKER_NETWORK_NETTY_BACKLOG =
      defaultKey(Name.WORKER_NETWORK_NETTY_BACKLOG, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_BOSS_THREADS =
      defaultKey(Name.WORKER_NETWORK_NETTY_BOSS_THREADS, 1);
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
      defaultKey(Name.WORKER_NETWORK_NETTY_BUFFER_RECEIVE, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_SEND =
      defaultKey(Name.WORKER_NETWORK_NETTY_BUFFER_SEND, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_CHANNEL =
      defaultKey(Name.WORKER_NETWORK_NETTY_CHANNEL, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
      defaultKey(Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "MAPPED");
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
      defaultKey(Name.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, 2);
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
      defaultKey(Name.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, 15);
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_HIGH =
      defaultKey(Name.WORKER_NETWORK_NETTY_WATERMARK_HIGH, "32KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_LOW =
      defaultKey(Name.WORKER_NETWORK_NETTY_WATERMARK_LOW, "8KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_WORKER_THREADS =
      defaultKey(Name.WORKER_NETWORK_NETTY_WORKER_THREADS, 0);
  public static final PropertyKey WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      defaultKey(Name.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      defaultKey(Name.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
      defaultKey(Name.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
      defaultKey(Name.WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
      defaultKey(Name.WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX =
      defaultKey(Name.WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
      defaultKey(Name.WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX, 128);

  public static final PropertyKey WORKER_PRINCIPAL = defaultKey(Name.WORKER_PRINCIPAL, null);
  public static final PropertyKey WORKER_RPC_PORT = defaultKey(Name.WORKER_RPC_PORT, 29998);
  public static final PropertyKey WORKER_SESSION_TIMEOUT_MS =
      defaultKey(Name.WORKER_SESSION_TIMEOUT_MS, 60000);
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
      defaultKey(Name.WORKER_TIERED_STORE_BLOCK_LOCK_READERS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCKS =
      defaultKey(Name.WORKER_TIERED_STORE_BLOCK_LOCKS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_ALIAS =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL0_ALIAS, "MEM");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_PATH =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, "/mnt/ramdisk");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "${alluxio.worker.memory.size}");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO, "0.1");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_ALIAS =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL1_ALIAS, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_PATH =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_ALIAS =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL2_ALIAS, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_PATH =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
      defaultKey(Name.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVELS =
      defaultKey(Name.WORKER_TIERED_STORE_LEVELS, 1);
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_ENABLED =
      defaultKey(Name.WORKER_TIERED_STORE_RESERVER_ENABLED, false);
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
      defaultKey(Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_RETRY =
      defaultKey(Name.WORKER_TIERED_STORE_RETRY, 3);
  public static final PropertyKey WORKER_WEB_BIND_HOST =
      defaultKey(Name.WORKER_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_WEB_HOSTNAME = defaultKey(Name.WORKER_WEB_HOSTNAME, null);
  public static final PropertyKey WORKER_WEB_PORT = defaultKey(Name.WORKER_WEB_PORT, 30000);

  //
  // Proxy related properties
  //
  public static final PropertyKey PROXY_STREAM_CACHE_TIMEOUT_MS =
      defaultKey(Name.PROXY_STREAM_CACHE_TIMEOUT_MS, 3600000);
  public static final PropertyKey PROXY_WEB_BIND_HOST =
      defaultKey(Name.PROXY_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey PROXY_WEB_HOSTNAME = defaultKey(Name.PROXY_WEB_HOSTNAME, null);
  public static final PropertyKey PROXY_WEB_PORT = defaultKey(Name.PROXY_WEB_PORT, 39999);

  //
  // User related properties
  //
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_THREADS =
      defaultKey(Name.USER_BLOCK_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
      defaultKey(Name.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "8MB");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_BLOCK_REMOTE_READER_CLASS =
      defaultKey(Name.USER_BLOCK_REMOTE_READER_CLASS,
          "alluxio.client.netty.NettyRemoteBlockReader");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_BLOCK_REMOTE_WRITER_CLASS =
      defaultKey(Name.USER_BLOCK_REMOTE_WRITER_CLASS,
          "alluxio.client.netty.NettyRemoteBlockWriter");
  public static final PropertyKey USER_BLOCK_SIZE_BYTES_DEFAULT =
      defaultKey(Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "512MB");
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_THREADS =
      defaultKey(Name.USER_BLOCK_WORKER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX =
      defaultKey(Name.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX, 128);
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      defaultKey(Name.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS, 300 * Constants.SECOND_MS);
  public static final PropertyKey USER_DATE_FORMAT_PATTERN =
      defaultKey(Name.USER_DATE_FORMAT_PATTERN, "MM-dd-yyyy HH:mm:ss:SSS");
  public static final PropertyKey USER_FAILED_SPACE_REQUEST_LIMITS =
      defaultKey(Name.USER_FAILED_SPACE_REQUEST_LIMITS, 3);
  public static final PropertyKey USER_FILE_BUFFER_BYTES =
      defaultKey(Name.USER_FILE_BUFFER_BYTES, "1MB");
  public static final PropertyKey USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
      defaultKey(Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, true);
  public static final PropertyKey USER_FILE_MASTER_CLIENT_THREADS =
      defaultKey(Name.USER_FILE_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_FILE_PASSIVE_CACHE_ENABLED =
      defaultKey(Name.USER_FILE_PASSIVE_CACHE_ENABLED, true);
  public static final PropertyKey USER_FILE_READ_TYPE_DEFAULT =
      defaultKey(Name.USER_FILE_READ_TYPE_DEFAULT, "CACHE_PROMOTE");
  public static final PropertyKey USER_FILE_SEEK_BUFFER_SIZE_BYTES =
      defaultKey(Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES, "1MB");
  public static final PropertyKey USER_FILE_WAITCOMPLETED_POLL_MS =
      defaultKey(Name.USER_FILE_WAITCOMPLETED_POLL_MS, 1000);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_THREADS =
      defaultKey(Name.USER_FILE_WORKER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX =
      defaultKey(Name.USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX, 128);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      defaultKey(Name.USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS, 300 * Constants.SECOND_MS);
  public static final PropertyKey USER_FILE_WRITE_LOCATION_POLICY =
      defaultKey(Name.USER_FILE_WRITE_LOCATION_POLICY,
          "alluxio.client.file.policy.LocalFirstPolicy");
  public static final PropertyKey USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
      defaultKey(Name.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES, "0MB");
  public static final PropertyKey USER_FILE_WRITE_TYPE_DEFAULT =
      defaultKey(Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
  public static final PropertyKey USER_FILE_WRITE_TIER_DEFAULT =
      defaultKey(Name.USER_FILE_WRITE_TIER_DEFAULT, Constants.FIRST_TIER);
  public static final PropertyKey USER_HEARTBEAT_INTERVAL_MS =
      defaultKey(Name.USER_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey USER_HOSTNAME = defaultKey(Name.USER_HOSTNAME, null);
  public static final PropertyKey USER_LINEAGE_ENABLED =
      defaultKey(Name.USER_LINEAGE_ENABLED, false);
  public static final PropertyKey USER_LINEAGE_MASTER_CLIENT_THREADS =
      defaultKey(Name.USER_LINEAGE_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_LOCAL_READER_PACKET_SIZE_BYTES =
      defaultKey(Name.USER_LOCAL_READER_PACKET_SIZE_BYTES, "8MB");
  public static final PropertyKey USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
      defaultKey(Name.USER_LOCAL_WRITER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL =
      defaultKey(Name.USER_NETWORK_NETTY_CHANNEL, null);
  public static final PropertyKey USER_NETWORK_NETTY_TIMEOUT_MS =
      defaultKey(Name.USER_NETWORK_NETTY_TIMEOUT_MS, 30000);
  public static final PropertyKey USER_NETWORK_NETTY_WORKER_THREADS =
      defaultKey(Name.USER_NETWORK_NETTY_WORKER_THREADS, 0);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
      defaultKey(Name.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX, 1024);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
      defaultKey(Name.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS, 300 * Constants.SECOND_MS);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED =
      defaultKey(Name.USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED, false);
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES =
      defaultKey(Name.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      defaultKey(Name.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      defaultKey(Name.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey USER_NETWORK_NETTY_READER_CANCEL_ENABLED =
      defaultKey(Name.USER_NETWORK_NETTY_READER_CANCEL_ENABLED, true);
  public static final PropertyKey USER_PACKET_STREAMING_ENABLED =
      defaultKey(Name.USER_PACKET_STREAMING_ENABLED, true);
  public static final PropertyKey USER_RPC_RETRY_BASE_SLEEP_MS =
      defaultKey(Name.USER_RPC_RETRY_BASE_SLEEP_MS, 50);
  public static final PropertyKey USER_RPC_RETRY_MAX_NUM_RETRY =
      defaultKey(Name.USER_RPC_RETRY_MAX_NUM_RETRY, 20);
  public static final PropertyKey USER_RPC_RETRY_MAX_SLEEP_MS =
      defaultKey(Name.USER_RPC_RETRY_MAX_SLEEP_MS, 5000);
  // Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_ENABLED =
      defaultKey(Name.USER_UFS_DELEGATION_ENABLED, true);
  // Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
      defaultKey(Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES, "8MB");
  public static final PropertyKey USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
      defaultKey(Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES, "2MB");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_UFS_FILE_READER_CLASS =
      defaultKey(Name.USER_UFS_FILE_READER_CLASS,
          "alluxio.client.netty.NettyUnderFileSystemFileReader");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_UFS_FILE_WRITER_CLASS =
      defaultKey(Name.USER_UFS_FILE_WRITER_CLASS,
          "alluxio.client.netty.NettyUnderFileSystemFileWriter");
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY =
      defaultKey(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
          "alluxio.client.file.policy.LocalFirstPolicy");
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
      defaultKey(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS, 1);
  public static final PropertyKey USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
      defaultKey(Name.USER_UFS_BLOCK_READ_CONCURRENCY_MAX, Integer.MAX_VALUE);
  public static final PropertyKey USER_UFS_BLOCK_OPEN_TIMEOUT_MS =
      defaultKey(Name.USER_UFS_BLOCK_OPEN_TIMEOUT_MS, 300000);
  public static final PropertyKey USER_SHORT_CIRCUIT_ENABLED =
      defaultKey(Name.USER_SHORT_CIRCUIT_ENABLED, true);

  //
  // FUSE integration related properties
  //
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  public static final PropertyKey FUSE_CACHED_PATHS_MAX =
      defaultKey(Name.FUSE_CACHED_PATHS_MAX, 500);
  /** Have the fuse process log every FS request. */
  public static final PropertyKey FUSE_DEBUG_ENABLED = defaultKey(Name.FUSE_DEBUG_ENABLED, false);

  /** FUSE file system name. */
  public static final PropertyKey FUSE_FS_NAME = defaultKey(Name.FUSE_FS_NAME, "alluxio-fuse");
  public static final PropertyKey FUSE_FS_ROOT = defaultKey(Name.FUSE_FS_ROOT, "/");
  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  public static final PropertyKey FUSE_MAXWRITE_BYTES =
      defaultKey(Name.FUSE_MAXWRITE_BYTES, 131072);
  public static final PropertyKey FUSE_MOUNT_DEFAULT =
      defaultKey(Name.FUSE_MOUNT_DEFAULT, "/mnt/alluxio");

  //
  // Security related properties
  //
  public static final PropertyKey SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
      defaultKey(Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS, null);
  public static final PropertyKey SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS =
      defaultKey(Name.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS, "600000");
  public static final PropertyKey SECURITY_AUTHENTICATION_TYPE =
      defaultKey(Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      defaultKey(Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true);
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
      defaultKey(Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "supergroup");
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_UMASK =
      defaultKey(Name.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "022");
  public static final PropertyKey SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
      defaultKey(Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS, "60000");
  public static final PropertyKey SECURITY_GROUP_MAPPING_CLASS =
      defaultKey(Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.ShellBasedUnixGroupsMapping");
  public static final PropertyKey SECURITY_LOGIN_USERNAME =
      defaultKey(Name.SECURITY_LOGIN_USERNAME, null);

  //
  // Mesos and Yarn related properties
  //
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_CPU =
      defaultKey(Name.INTEGRATION_MASTER_RESOURCE_CPU, 1);
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_MEM =
      defaultKey(Name.INTEGRATION_MASTER_RESOURCE_MEM, "1024MB");
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_JAR_URL =
      defaultKey(Name.INTEGRATION_MESOS_ALLUXIO_JAR_URL, String.format(
          "http://downloads.alluxio.org/downloads/files/${%s}/" + "alluxio-${%s}-bin.tar.gz",
          Name.VERSION, Name.VERSION));
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NAME =
      defaultKey(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME, "AlluxioMaster");
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT =
      defaultKey(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT, 1);
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_WORKER_NAME =
      defaultKey(Name.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME, "AlluxioWorker");
  public static final PropertyKey INTEGRATION_MESOS_JDK_PATH =
      defaultKey(Name.INTEGRATION_MESOS_JDK_PATH, "jdk1.7.0_79");
  public static final PropertyKey INTEGRATION_MESOS_JDK_URL =
      defaultKey(Name.INTEGRATION_MESOS_JDK_URL,
          "https://alluxio-mesos.s3.amazonaws.com/jdk-7u79-linux-x64.tar.gz");
  public static final PropertyKey INTEGRATION_MESOS_PRINCIPAL =
      defaultKey(Name.INTEGRATION_MESOS_PRINCIPAL, "alluxio");
  public static final PropertyKey INTEGRATION_MESOS_ROLE =
      defaultKey(Name.INTEGRATION_MESOS_ROLE, "*");
  public static final PropertyKey INTEGRATION_MESOS_SECRET =
      defaultKey(Name.INTEGRATION_MESOS_SECRET, null);
  public static final PropertyKey INTEGRATION_MESOS_USER =
      defaultKey(Name.INTEGRATION_MESOS_USER, "");
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_CPU =
      defaultKey(Name.INTEGRATION_WORKER_RESOURCE_CPU, 1);
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_MEM =
      defaultKey(Name.INTEGRATION_WORKER_RESOURCE_MEM, "1024MB");
  public static final PropertyKey INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
      defaultKey(Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX, 1);

  /**
   * A nested class to hold named string constants for their corresponding enum values.
   * Used for setting configuration in integration tests.
   */
  @ThreadSafe
  public static final class Name {
    public static final String CONF_DIR = "alluxio.conf.dir";
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
    public static final String INTEGRATION_MESOS_ALLUXIO_JAR_URL =
        "alluxio.integration.mesos.alluxio.jar.url";
    public static final String INTEGRATION_MESOS_JDK_PATH = "alluxio.integration.mesos.jdk.path";
    public static final String INTEGRATION_MESOS_JDK_URL = "alluxio.integration.mesos.jdk.url";
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
    public static final String WORK_DIR = "alluxio.work.dir";
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
    public static final String UNDERFS_ALLOW_SET_OWNER_FAILURE =
        "alluxio.underfs.allow.set.owner.failure";
    public static final String UNDERFS_LISTING_LENGTH = "alluxio.underfs.listing.length";
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
    public static final String UNDERFS_HDFS_REMOTE = "alluxio.underfs.hdfs.remote";
    public static final String UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
        "alluxio.underfs.object.store.mount.shared.publicly";
    public static final String UNDERFS_OSS_CONNECT_MAX = "alluxio.underfs.oss.connection.max";
    public static final String UNDERFS_OSS_CONNECT_TIMEOUT =
        "alluxio.underfs.oss.connection.timeout.ms";
    public static final String UNDERFS_OSS_CONNECT_TTL = "alluxio.underfs.oss.connection.ttl";
    public static final String UNDERFS_OSS_SOCKET_TIMEOUT = "alluxio.underfs.oss.socket.timeout.ms";
    public static final String UNDERFS_S3A_INHERIT_ACL = "alluxio.underfs.s3a.inherit_acl";
    public static final String UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
        "alluxio.underfs.s3a.consistency.timeout.ms";
    public static final String UNDERFS_S3A_REQUEST_TIMEOUT_MS =
        "alluxio.underfs.s3a.request.timeout.ms";
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
    public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
    public static final String SWIFT_PASSWORD_KEY = "fs.swift.password";
    public static final String SWIFT_SIMULATION = "fs.swift.simulation";
    public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
    public static final String SWIFT_USER_KEY = "fs.swift.user";
    public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";
    public static final String SWIFT_REGION_KEY = "fs.swift.region";

    //
    // Master related properties
    //
    public static final String MASTER_ADDRESS = "alluxio.master.address";
    public static final String MASTER_BIND_HOST = "alluxio.master.bind.host";
    public static final String MASTER_CONNECTION_TIMEOUT_MS =
        "alluxio.master.connection.timeout.ms";
    public static final String MASTER_FILE_ASYNC_PERSIST_HANDLER =
        "alluxio.master.file.async.persist.handler";
    public static final String MASTER_FORMAT_FILE_PREFIX = "alluxio.master.format.file_prefix";
    public static final String MASTER_HEARTBEAT_INTERVAL_MS =
        "alluxio.master.heartbeat.interval.ms";
    public static final String MASTER_HOSTNAME = "alluxio.master.hostname";
    public static final String MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
        "alluxio.master.journal.flush.batch.time.ms";
    public static final String MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
        "alluxio.master.journal.flush.timeout.ms";
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
    public static final String MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
        "alluxio.master.startup.consistency.check.enabled";
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
    public static final String WORKER_FILE_BUFFER_SIZE = "alluxio.worker.file.buffer.size";
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
    public static final String WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
        "alluxio.worker.network.netty.writer.buffer.size.packets";
    public static final String WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
        "alluxio.worker.network.netty.reader.buffer.size.packets";
    public static final String WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
        "alluxio.worker.network.netty.reader.packet.size.bytes";
    public static final String WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
        "alluxio.worker.network.netty.block.reader.threads.max";
    public static final String WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
        "alluxio.worker.network.netty.block.writer.threads.max";
    public static final String WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX =
        "alluxio.worker.network.netty.file.reader.threads.max";
    public static final String WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
        "alluxio.worker.network.netty.file.writer.threads.max";
    public static final String WORKER_PRINCIPAL = "alluxio.worker.principal";
    public static final String WORKER_RPC_PORT = "alluxio.worker.port";
    public static final String WORKER_SESSION_TIMEOUT_MS = "alluxio.worker.session.timeout.ms";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
        "alluxio.worker.tieredstore.block.lock.readers";
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
    public static final String WORKER_TIERED_STORE_RETRY = "alluxio.worker.tieredstore.retry";
    public static final String WORKER_WEB_BIND_HOST = "alluxio.worker.web.bind.host";
    public static final String WORKER_WEB_HOSTNAME = "alluxio.worker.web.hostname";
    public static final String WORKER_WEB_PORT = "alluxio.worker.web.port";

    //
    // Proxy related properties
    //
    public static final String PROXY_STREAM_CACHE_TIMEOUT_MS =
        "alluxio.proxy.stream.cache.timeout.ms";
    public static final String PROXY_WEB_BIND_HOST = "alluxio.proxy.web.bind.host";
    public static final String PROXY_WEB_HOSTNAME = "alluxio.proxy.web.hostname";
    public static final String PROXY_WEB_PORT = "alluxio.proxy.web.port";

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
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX =
        "alluxio.user.block.worker.client.pool.size.max";
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
        "alluxio.user.block.worker.client.pool.gc.threshold.ms";
    public static final String USER_DATE_FORMAT_PATTERN = "alluxio.user.date.format.pattern";
    public static final String USER_FAILED_SPACE_REQUEST_LIMITS =
        "alluxio.user.failed.space.request.limits";
    public static final String USER_FILE_BUFFER_BYTES = "alluxio.user.file.buffer.bytes";
    public static final String USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
        "alluxio.user.file.cache.partially.read.block";
    public static final String USER_FILE_MASTER_CLIENT_THREADS =
        "alluxio.user.file.master.client.threads";
    public static final String USER_FILE_PASSIVE_CACHE_ENABLED =
        "alluxio.user.file.passive.cache.enabled";
    public static final String USER_FILE_READ_TYPE_DEFAULT = "alluxio.user.file.readtype.default";
    public static final String USER_FILE_SEEK_BUFFER_SIZE_BYTES =
        "alluxio.user.file.seek.buffer.size.bytes";
    public static final String USER_FILE_WAITCOMPLETED_POLL_MS =
        "alluxio.user.file.waitcompleted.poll.ms";
    public static final String USER_FILE_WORKER_CLIENT_THREADS =
        "alluxio.user.file.worker.client.threads";
    public static final String USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX =
        "alluxio.user.file.worker.client.pool.size.max";
    public static final String USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
        "alluxio.user.file.worker.client.pool.gc.threshold.ms";
    public static final String USER_FILE_WRITE_LOCATION_POLICY =
        "alluxio.user.file.write.location.policy.class";
    public static final String USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
        "alluxio.user.file.write.avoid.eviction.policy.reserved.size.bytes";
    public static final String USER_FILE_WRITE_TYPE_DEFAULT = "alluxio.user.file.writetype.default";
    public static final String USER_FILE_WRITE_TIER_DEFAULT =
        "alluxio.user.file.write.tier.default";
    public static final String USER_HEARTBEAT_INTERVAL_MS = "alluxio.user.heartbeat.interval.ms";
    public static final String USER_HOSTNAME = "alluxio.user.hostname";
    public static final String USER_LINEAGE_ENABLED = "alluxio.user.lineage.enabled";
    public static final String USER_LINEAGE_MASTER_CLIENT_THREADS =
        "alluxio.user.lineage.master.client.threads";
    public static final String USER_LOCAL_READER_PACKET_SIZE_BYTES =
        "alluxio.user.local.reader.packet.size.bytes";
    public static final String USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
        "alluxio.user.local.writer.packet.size.bytes";
    public static final String USER_NETWORK_NETTY_CHANNEL = "alluxio.user.network.netty.channel";
    public static final String USER_NETWORK_NETTY_TIMEOUT_MS =
        "alluxio.user.network.netty.timeout.ms";
    public static final String USER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.user.network.netty.worker.threads";
    public static final String USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
        "alluxio.user.network.netty.channel.pool.size.max";
    public static final String USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
        "alluxio.user.network.netty.channel.pool.gc.threshold.ms";
    public static final String USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED =
        "alluxio.user.network.netty.channel.pool.disabled";
    public static final String USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES =
        "alluxio.user.network.netty.writer.packet.size.bytes";
    public static final String USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
        "alluxio.user.network.netty.writer.buffer.size.packets";
    public static final String USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
        "alluxio.user.network.netty.reader.buffer.size.packets";
    public static final String USER_NETWORK_NETTY_READER_CANCEL_ENABLED =
        "alluxio.user.network.netty.reader.cancel.enabled";
    public static final String USER_PACKET_STREAMING_ENABLED =
        "alluxio.user.packet.streaming.enabled";
    public static final String USER_RPC_RETRY_BASE_SLEEP_MS =
        "alluxio.user.rpc.retry.base.sleep.ms";
    public static final String USER_RPC_RETRY_MAX_NUM_RETRY =
        "alluxio.user.rpc.retry.max.num.retry";
    public static final String USER_RPC_RETRY_MAX_SLEEP_MS = "alluxio.user.rpc.retry.max.sleep.ms";
    public static final String USER_UFS_DELEGATION_ENABLED = "alluxio.user.ufs.delegation.enabled";
    public static final String USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.read.buffer.size.bytes";
    public static final String USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.write.buffer.size.bytes";
    public static final String USER_UFS_FILE_READER_CLASS = "alluxio.user.ufs.file.reader.class";
    public static final String USER_UFS_FILE_WRITER_CLASS = "alluxio.user.ufs.file.writer.class";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY =
        "alluxio.user.ufs.block.read.location.policy";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
        "alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards";
    public static final String USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
        "alluxio.user.ufs.block.read.concurrency.max";
    public static final String USER_UFS_BLOCK_OPEN_TIMEOUT_MS =
        "alluxio.user.ufs.block.open.timeout.ms";
    public static final String USER_SHORT_CIRCUIT_ENABLED = "alluxio.user.short.circuit.disabled";

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

  /** Property name. */
  private final String mName;

  /**
   * @param keyStr string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String keyStr) {
    return DEFAULT_KEYS_MAP.containsKey(keyStr) || ParameterizedPropertyKey.isValid(keyStr);
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param keyStr string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String keyStr) {
    PropertyKey key = DEFAULT_KEYS_MAP.get(keyStr);
    if (key != null) {
      return key;
    }
    return ParameterizedPropertyKey.fromString(keyStr);
  }

  /**
   * @return all default keys
   */
  public static Collection<PropertyKey> getDefaultKeys() {
    return DEFAULT_KEYS_MAP.values();
  }

  /**
   * Factory method to create a default property.
   *
   * @param propertyStr String of this property
   * @param defaultValue Default value of this property in compile time if not null
   */
  private static PropertyKey defaultKey(String propertyStr, Object defaultValue) {
    PropertyKey key = new PropertyKey(propertyStr);
    DEFAULT_KEYS_MAP.put(propertyStr, key);
    DEFAULT_VALUES.put(key, defaultValue);
    return key;
  }

  /**
   * @param property String of this property
   */
  PropertyKey(String property) {
    mName = property;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PropertyKey)) {
      return false;
    }
    PropertyKey that = (PropertyKey) o;
    return Objects.equal(mName, that.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName);
  }

  @Override
  public String toString() {
    return mName;
  }

  /**
   * @return the default value of a property key or null if no default value set
   */
  public String getDefaultValue() {
    Object value = DEFAULT_VALUES.get(this);
    if (value != null) {
      return value.toString();
    }
    return null;
  }

}
