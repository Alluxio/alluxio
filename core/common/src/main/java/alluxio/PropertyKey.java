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

import alluxio.exception.ExceptionMessage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration property keys. This class provides a set of pre-defined property keys.
 */
@ThreadSafe
public class PropertyKey {
  public static final PropertyKey CONF_DIR = create(ConstantPropertyKey.Name.CONF_DIR,
      String.format("${%s}/conf", ConstantPropertyKey.Name.HOME));
  public static final PropertyKey DEBUG = create(ConstantPropertyKey.Name.DEBUG, false);
  public static final PropertyKey HOME = create(ConstantPropertyKey.Name.HOME, "/opt/alluxio");
  public static final PropertyKey KEY_VALUE_ENABLED =
      create(ConstantPropertyKey.Name.KEY_VALUE_ENABLED, false);
  public static final PropertyKey KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
      create(ConstantPropertyKey.Name.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, "512MB");
  public static final PropertyKey LOGGER_TYPE =
      create(ConstantPropertyKey.Name.LOGGER_TYPE, "Console");
  public static final PropertyKey LOGS_DIR = create(ConstantPropertyKey.Name.LOGS_DIR,
      String.format("${%s}/logs", ConstantPropertyKey.Name.WORK_DIR));
  public static final PropertyKey METRICS_CONF_FILE =
      create(ConstantPropertyKey.Name.METRICS_CONF_FILE,
          String.format("${%s}/metrics.properties", ConstantPropertyKey.Name.CONF_DIR));
  public static final PropertyKey NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, 5000);
  public static final PropertyKey NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
      create(ConstantPropertyKey.Name.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX, "16MB");
  public static final PropertyKey SITE_CONF_DIR =
      create(ConstantPropertyKey.Name.SITE_CONF_DIR, "${user.home}/.alluxio/,/etc/alluxio/");

  public static final PropertyKey TEST_MODE = create(ConstantPropertyKey.Name.TEST_MODE, false);
  public static final PropertyKey VERSION =
      create(ConstantPropertyKey.Name.VERSION, ProjectConstants.VERSION);
  public static final PropertyKey WEB_RESOURCES = create(ConstantPropertyKey.Name.WEB_RESOURCES,
      String.format("${%s}/core/server/common/src/main/webapp", ConstantPropertyKey.Name.HOME));
  public static final PropertyKey WEB_THREADS = create(ConstantPropertyKey.Name.WEB_THREADS, 1);
  public static final PropertyKey WORK_DIR = create(ConstantPropertyKey.Name.WORK_DIR,
      String.format("${%s}", ConstantPropertyKey.Name.HOME));
  public static final PropertyKey ZOOKEEPER_ADDRESS =
      create(ConstantPropertyKey.Name.ZOOKEEPER_ADDRESS, null);
  public static final PropertyKey ZOOKEEPER_ELECTION_PATH =
      create(ConstantPropertyKey.Name.ZOOKEEPER_ELECTION_PATH, "/election");
  public static final PropertyKey ZOOKEEPER_ENABLED =
      create(ConstantPropertyKey.Name.ZOOKEEPER_ENABLED, false);
  public static final PropertyKey ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      create(ConstantPropertyKey.Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT, 10);
  public static final PropertyKey ZOOKEEPER_LEADER_PATH =
      create(ConstantPropertyKey.Name.ZOOKEEPER_LEADER_PATH, "/leader");

  //
  // UFS related properties
  //
  // Deprecated
  public static final PropertyKey UNDERFS_ADDRESS = create(ConstantPropertyKey.Name.UNDERFS_ADDRESS,
      String.format("${%s}/underFSStorage", ConstantPropertyKey.Name.WORK_DIR));
  public static final PropertyKey UNDERFS_ALLOW_SET_OWNER_FAILURE =
      create(ConstantPropertyKey.Name.UNDERFS_ALLOW_SET_OWNER_FAILURE, false);
  public static final PropertyKey UNDERFS_LISTING_LENGTH =
      create(ConstantPropertyKey.Name.UNDERFS_LISTING_LENGTH, 1000);
  public static final PropertyKey UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
      create(ConstantPropertyKey.Name.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING, "");
  public static final PropertyKey UNDERFS_GLUSTERFS_IMPL =
      create(ConstantPropertyKey.Name.UNDERFS_GLUSTERFS_IMPL,
          "org.apache.hadoop.fs.glusterfs.GlusterFileSystem");
  public static final PropertyKey UNDERFS_GLUSTERFS_MOUNTS =
      create(ConstantPropertyKey.Name.UNDERFS_GLUSTERFS_MOUNTS, null);
  public static final PropertyKey UNDERFS_GLUSTERFS_MR_DIR =
      create(ConstantPropertyKey.Name.UNDERFS_GLUSTERFS_MR_DIR, "glusterfs:///mapred/system");
  public static final PropertyKey UNDERFS_GLUSTERFS_VOLUMES =
      create(ConstantPropertyKey.Name.UNDERFS_GLUSTERFS_VOLUMES, null);
  public static final PropertyKey UNDERFS_HDFS_CONFIGURATION =
      create(ConstantPropertyKey.Name.UNDERFS_HDFS_CONFIGURATION,
          String.format("${%s}/core-site.xml", ConstantPropertyKey.Name.CONF_DIR));
  public static final PropertyKey UNDERFS_HDFS_IMPL =
      create(ConstantPropertyKey.Name.UNDERFS_HDFS_IMPL,
          "org.apache.hadoop.hdfs.DistributedFileSystem");
  public static final PropertyKey UNDERFS_HDFS_PREFIXES =
      create(ConstantPropertyKey.Name.UNDERFS_HDFS_PREFIXES, "hdfs://,glusterfs:///,maprfs:///");
  public static final PropertyKey UNDERFS_HDFS_REMOTE =
      create(ConstantPropertyKey.Name.UNDERFS_HDFS_REMOTE, false);
  public static final PropertyKey UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
      create(ConstantPropertyKey.Name.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY, false);
  public static final PropertyKey UNDERFS_OSS_CONNECT_MAX =
      create(ConstantPropertyKey.Name.UNDERFS_OSS_CONNECT_MAX, 1024);
  public static final PropertyKey UNDERFS_OSS_CONNECT_TIMEOUT =
      create(ConstantPropertyKey.Name.UNDERFS_OSS_CONNECT_TIMEOUT, 50000);
  public static final PropertyKey UNDERFS_OSS_CONNECT_TTL =
      create(ConstantPropertyKey.Name.UNDERFS_OSS_CONNECT_TTL, -1);
  public static final PropertyKey UNDERFS_OSS_SOCKET_TIMEOUT =
      create(ConstantPropertyKey.Name.UNDERFS_OSS_SOCKET_TIMEOUT, 50000);
  public static final PropertyKey UNDERFS_S3_ADMIN_THREADS_MAX =
      create(ConstantPropertyKey.Name.UNDERFS_S3_ADMIN_THREADS_MAX, 20);
  public static final PropertyKey UNDERFS_S3_DISABLE_DNS_BUCKETS =
      create(ConstantPropertyKey.Name.UNDERFS_S3_DISABLE_DNS_BUCKETS, false);
  public static final PropertyKey UNDERFS_S3_ENDPOINT =
      create(ConstantPropertyKey.Name.UNDERFS_S3_ENDPOINT, null);
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTP_PORT =
      create(ConstantPropertyKey.Name.UNDERFS_S3_ENDPOINT_HTTP_PORT, null);
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTPS_PORT =
      create(ConstantPropertyKey.Name.UNDERFS_S3_ENDPOINT_HTTPS_PORT, null);
  public static final PropertyKey UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
      create(ConstantPropertyKey.Name.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING, "");
  public static final PropertyKey UNDERFS_S3_PROXY_HOST =
      create(ConstantPropertyKey.Name.UNDERFS_S3_PROXY_HOST, null);
  public static final PropertyKey UNDERFS_S3_PROXY_HTTPS_ONLY =
      create(ConstantPropertyKey.Name.UNDERFS_S3_PROXY_HTTPS_ONLY, true);
  public static final PropertyKey UNDERFS_S3_PROXY_PORT =
      create(ConstantPropertyKey.Name.UNDERFS_S3_PROXY_PORT, null);
  public static final PropertyKey UNDERFS_S3_THREADS_MAX =
      create(ConstantPropertyKey.Name.UNDERFS_S3_THREADS_MAX, 40);
  public static final PropertyKey UNDERFS_S3_UPLOAD_THREADS_MAX =
      create(ConstantPropertyKey.Name.UNDERFS_S3_UPLOAD_THREADS_MAX, 20);
  public static final PropertyKey UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS, 60000);
  public static final PropertyKey UNDERFS_S3A_INHERIT_ACL =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_INHERIT_ACL, true);
  public static final PropertyKey UNDERFS_S3A_REQUEST_TIMEOUT =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_REQUEST_TIMEOUT_MS, 60000);
  public static final PropertyKey UNDERFS_S3A_SECURE_HTTP_ENABLED =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_SECURE_HTTP_ENABLED, false);
  public static final PropertyKey UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED, false);
  public static final PropertyKey UNDERFS_S3A_SOCKET_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.UNDERFS_S3A_SOCKET_TIMEOUT_MS, 50000);

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  public static final PropertyKey GCS_ACCESS_KEY =
      create(ConstantPropertyKey.Name.GCS_ACCESS_KEY, null);
  public static final PropertyKey GCS_SECRET_KEY =
      create(ConstantPropertyKey.Name.GCS_SECRET_KEY, null);
  public static final PropertyKey OSS_ACCESS_KEY =
      create(ConstantPropertyKey.Name.OSS_ACCESS_KEY, null);
  public static final PropertyKey OSS_ENDPOINT_KEY =
      create(ConstantPropertyKey.Name.OSS_ENDPOINT_KEY, null);
  public static final PropertyKey OSS_SECRET_KEY =
      create(ConstantPropertyKey.Name.OSS_SECRET_KEY, null);
  public static final PropertyKey S3A_ACCESS_KEY =
      create(ConstantPropertyKey.Name.S3A_ACCESS_KEY, null);
  public static final PropertyKey S3A_SECRET_KEY =
      create(ConstantPropertyKey.Name.S3A_SECRET_KEY, null);
  public static final PropertyKey S3N_ACCESS_KEY =
      create(ConstantPropertyKey.Name.S3N_ACCESS_KEY, null);
  public static final PropertyKey S3N_SECRET_KEY =
      create(ConstantPropertyKey.Name.S3N_SECRET_KEY, null);
  public static final PropertyKey SWIFT_API_KEY =
      create(ConstantPropertyKey.Name.SWIFT_API_KEY, null);
  public static final PropertyKey SWIFT_AUTH_METHOD_KEY =
      create(ConstantPropertyKey.Name.SWIFT_AUTH_METHOD_KEY, null);
  public static final PropertyKey SWIFT_AUTH_URL_KEY =
      create(ConstantPropertyKey.Name.SWIFT_AUTH_URL_KEY, null);
  public static final PropertyKey SWIFT_PASSWORD_KEY =
      create(ConstantPropertyKey.Name.SWIFT_PASSWORD_KEY, null);
  public static final PropertyKey SWIFT_SIMULATION =
      create(ConstantPropertyKey.Name.SWIFT_SIMULATION, null);
  public static final PropertyKey SWIFT_TENANT_KEY =
      create(ConstantPropertyKey.Name.SWIFT_TENANT_KEY, null);
  public static final PropertyKey SWIFT_USE_PUBLIC_URI_KEY =
      create(ConstantPropertyKey.Name.SWIFT_USE_PUBLIC_URI_KEY, null);
  public static final PropertyKey SWIFT_USER_KEY =
      create(ConstantPropertyKey.Name.SWIFT_USER_KEY, null);
  public static final PropertyKey SWIFT_REGION_KEY =
      create(ConstantPropertyKey.Name.SWIFT_REGION_KEY, null);

  //
  // Mount table related properties
  //
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_ALLUXIO =
      create(ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO, "/", "root");
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_OPTION =
      create(ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_OPTION, null, "root");
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_READONLY =
      create(ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_READONLY, false, "root");
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_SHARED =
      create(ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_SHARED, true, "root");
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_UFS =
      create(ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_UFS,
          String.format("${%s}", ConstantPropertyKey.Name.UNDERFS_ADDRESS), "root");

  //
  // Master related properties
  //
  public static final PropertyKey MASTER_ADDRESS =
      create(ConstantPropertyKey.Name.MASTER_ADDRESS, null);
  public static final PropertyKey MASTER_BIND_HOST =
      create(ConstantPropertyKey.Name.MASTER_BIND_HOST, "0.0.0.0");
  public static final PropertyKey MASTER_CONNECTION_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.MASTER_CONNECTION_TIMEOUT_MS, 0);
  public static final PropertyKey MASTER_FILE_ASYNC_PERSIST_HANDLER =
      create(ConstantPropertyKey.Name.MASTER_FILE_ASYNC_PERSIST_HANDLER,
          "alluxio.master.file.async.DefaultAsyncPersistHandler");
  public static final PropertyKey MASTER_FORMAT_FILE_PREFIX =
      create(ConstantPropertyKey.Name.MASTER_FORMAT_FILE_PREFIX, "_format_");
  public static final PropertyKey MASTER_HEARTBEAT_INTERVAL_MS =
      create(ConstantPropertyKey.Name.MASTER_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey MASTER_HOSTNAME =
      create(ConstantPropertyKey.Name.MASTER_HOSTNAME, null);
  public static final PropertyKey MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, 5);
  public static final PropertyKey MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, 300000);
  public static final PropertyKey MASTER_JOURNAL_FOLDER =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_FOLDER,
          String.format("${%s}/journal", ConstantPropertyKey.Name.WORK_DIR));
  public static final PropertyKey MASTER_JOURNAL_FORMATTER_CLASS =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_FORMATTER_CLASS,
          "alluxio.master.journal.ProtoBufJournalFormatter");
  public static final PropertyKey MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "10MB");
  public static final PropertyKey MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 5000);
  public static final PropertyKey MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      create(ConstantPropertyKey.Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, 1000);
  public static final PropertyKey MASTER_KEYTAB_KEY_FILE =
      create(ConstantPropertyKey.Name.MASTER_KEYTAB_KEY_FILE, null);
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_CLASS =
      create(ConstantPropertyKey.Name.MASTER_LINEAGE_CHECKPOINT_CLASS,
          "alluxio.master.lineage.checkpoint.CheckpointLatestPlanner");
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS =
      create(ConstantPropertyKey.Name.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS, 300000);
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS =
      create(ConstantPropertyKey.Name.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS, 300000);
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_LOG_PATH =
      create(ConstantPropertyKey.Name.MASTER_LINEAGE_RECOMPUTE_LOG_PATH,
          String.format("${%s}/recompute.log", ConstantPropertyKey.Name.LOGS_DIR));
  public static final PropertyKey MASTER_PRINCIPAL =
      create(ConstantPropertyKey.Name.MASTER_PRINCIPAL, null);
  // deprecated since version 1.4 and will be removed in version 2.0
  // use USER_RPC_RETRY_MAX_NUM_RETRY instead
  public static final PropertyKey MASTER_RETRY = create(ConstantPropertyKey.Name.MASTER_RETRY,
      String.format("${%s}", ConstantPropertyKey.Name.USER_RPC_RETRY_MAX_NUM_RETRY));
  public static final PropertyKey MASTER_RPC_PORT =
      create(ConstantPropertyKey.Name.MASTER_RPC_PORT, 19998);
  public static final PropertyKey MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
      create(ConstantPropertyKey.Name.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED, true);
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
      create(ConstantPropertyKey.Name.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS, "MEM");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
      create(ConstantPropertyKey.Name.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS, "SSD");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
      create(ConstantPropertyKey.Name.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS, "HDD");
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVELS =
      create(ConstantPropertyKey.Name.MASTER_TIERED_STORE_GLOBAL_LEVELS, 3);
  public static final PropertyKey MASTER_TTL_CHECKER_INTERVAL_MS =
      create(ConstantPropertyKey.Name.MASTER_TTL_CHECKER_INTERVAL_MS, 3600000);
  public static final PropertyKey MASTER_WEB_BIND_HOST =
      create(ConstantPropertyKey.Name.MASTER_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey MASTER_WEB_HOSTNAME =
      create(ConstantPropertyKey.Name.MASTER_WEB_HOSTNAME, null);
  public static final PropertyKey MASTER_WEB_PORT =
      create(ConstantPropertyKey.Name.MASTER_WEB_PORT, 19999);
  public static final PropertyKey MASTER_WHITELIST =
      create(ConstantPropertyKey.Name.MASTER_WHITELIST, "/");
  public static final PropertyKey MASTER_WORKER_THREADS_MAX =
      create(ConstantPropertyKey.Name.MASTER_WORKER_THREADS_MAX, 2048);
  public static final PropertyKey MASTER_WORKER_THREADS_MIN =
      create(ConstantPropertyKey.Name.MASTER_WORKER_THREADS_MIN, 512);
  public static final PropertyKey MASTER_WORKER_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.MASTER_WORKER_TIMEOUT_MS, 300000);

  //
  // Worker related properties
  //
  public static final PropertyKey WORKER_ALLOCATOR_CLASS =
      create(ConstantPropertyKey.Name.WORKER_ALLOCATOR_CLASS,
          "alluxio.worker.block.allocator.MaxFreeAllocator");
  public static final PropertyKey WORKER_BIND_HOST =
      create(ConstantPropertyKey.Name.WORKER_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      create(ConstantPropertyKey.Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, 60000);
  public static final PropertyKey WORKER_BLOCK_THREADS_MAX =
      create(ConstantPropertyKey.Name.WORKER_BLOCK_THREADS_MAX, 2048);
  public static final PropertyKey WORKER_BLOCK_THREADS_MIN =
      create(ConstantPropertyKey.Name.WORKER_BLOCK_THREADS_MIN, 256);
  public static final PropertyKey WORKER_DATA_BIND_HOST =
      create(ConstantPropertyKey.Name.WORKER_DATA_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_DATA_FOLDER =
      create(ConstantPropertyKey.Name.WORKER_DATA_FOLDER, "/alluxioworker/");
  public static final PropertyKey WORKER_DATA_HOSTNAME =
      create(ConstantPropertyKey.Name.WORKER_DATA_HOSTNAME, null);
  public static final PropertyKey WORKER_DATA_PORT =
      create(ConstantPropertyKey.Name.WORKER_DATA_PORT, 29999);
  public static final PropertyKey WORKER_DATA_SERVER_CLASS =
      create(ConstantPropertyKey.Name.WORKER_DATA_SERVER_CLASS,
          "alluxio.worker.netty.NettyDataServer");
  public static final PropertyKey WORKER_DATA_TMP_FOLDER =
      create(ConstantPropertyKey.Name.WORKER_DATA_TMP_FOLDER, ".tmp_blocks");
  public static final PropertyKey WORKER_DATA_TMP_SUBDIR_MAX =
      create(ConstantPropertyKey.Name.WORKER_DATA_TMP_SUBDIR_MAX, 1024);
  public static final PropertyKey WORKER_EVICTOR_CLASS =
      create(ConstantPropertyKey.Name.WORKER_EVICTOR_CLASS,
          "alluxio.worker.block.evictor.LRUEvictor");
  public static final PropertyKey WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
      create(ConstantPropertyKey.Name.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR, 2.0);
  public static final PropertyKey WORKER_EVICTOR_LRFU_STEP_FACTOR =
      create(ConstantPropertyKey.Name.WORKER_EVICTOR_LRFU_STEP_FACTOR, 0.25);
  public static final PropertyKey WORKER_FILE_PERSIST_POOL_SIZE =
      create(ConstantPropertyKey.Name.WORKER_FILE_PERSIST_POOL_SIZE, 64);
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT =
      create(ConstantPropertyKey.Name.WORKER_FILE_PERSIST_RATE_LIMIT, "2GB");
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
      create(ConstantPropertyKey.Name.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED, false);
  public static final PropertyKey WORKER_FILE_BUFFER_SIZE =
      create(ConstantPropertyKey.Name.WORKER_FILE_BUFFER_SIZE, "1MB");
  public static final PropertyKey WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
      create(ConstantPropertyKey.Name.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_HOSTNAME =
      create(ConstantPropertyKey.Name.WORKER_HOSTNAME, null);
  public static final PropertyKey WORKER_KEYTAB_FILE =
      create(ConstantPropertyKey.Name.WORKER_KEYTAB_FILE, null);
  public static final PropertyKey WORKER_MEMORY_SIZE =
      create(ConstantPropertyKey.Name.WORKER_MEMORY_SIZE, "1GB");
  public static final PropertyKey WORKER_NETWORK_NETTY_BACKLOG =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BACKLOG, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_BOSS_THREADS =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BOSS_THREADS, 1);
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BUFFER_RECEIVE, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_SEND =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BUFFER_SEND, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_CHANNEL =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_CHANNEL, null);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "MAPPED");
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, 2);
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, 15);
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_HIGH =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_WATERMARK_HIGH, "32KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_LOW =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_WATERMARK_LOW, "8KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_WORKER_THREADS =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_WORKER_THREADS, 0);
  public static final PropertyKey WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX, 128);
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
      create(ConstantPropertyKey.Name.WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX, 128);

  public static final PropertyKey WORKER_PRINCIPAL =
      create(ConstantPropertyKey.Name.WORKER_PRINCIPAL, null);
  public static final PropertyKey WORKER_RPC_PORT =
      create(ConstantPropertyKey.Name.WORKER_RPC_PORT, 29998);
  public static final PropertyKey WORKER_SESSION_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.WORKER_SESSION_TIMEOUT_MS, 60000);
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_BLOCK_LOCK_READERS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCKS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_BLOCK_LOCKS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_ALIAS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL0_ALIAS, "MEM");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_PATH =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, "/mnt/ramdisk");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
          "${alluxio.worker.memory.size}");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO, "0.1");
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_ALIAS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL1_ALIAS, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_PATH =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_ALIAS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL2_ALIAS, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_PATH =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO, null);
  public static final PropertyKey WORKER_TIERED_STORE_LEVELS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_LEVELS, 1);
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_ENABLED =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_RESERVER_ENABLED, false);
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, 1000);
  public static final PropertyKey WORKER_TIERED_STORE_RETRY =
      create(ConstantPropertyKey.Name.WORKER_TIERED_STORE_RETRY, 3);
  public static final PropertyKey WORKER_WEB_BIND_HOST =
      create(ConstantPropertyKey.Name.WORKER_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey WORKER_WEB_HOSTNAME =
      create(ConstantPropertyKey.Name.WORKER_WEB_HOSTNAME, null);
  public static final PropertyKey WORKER_WEB_PORT =
      create(ConstantPropertyKey.Name.WORKER_WEB_PORT, 30000);

  //
  // Proxy related properties
  //
  public static final PropertyKey PROXY_STREAM_CACHE_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.PROXY_STREAM_CACHE_TIMEOUT_MS, 3600000);
  public static final PropertyKey PROXY_WEB_BIND_HOST =
      create(ConstantPropertyKey.Name.PROXY_WEB_BIND_HOST, "0.0.0.0");
  public static final PropertyKey PROXY_WEB_HOSTNAME =
      create(ConstantPropertyKey.Name.PROXY_WEB_HOSTNAME, null);
  public static final PropertyKey PROXY_WEB_PORT =
      create(ConstantPropertyKey.Name.PROXY_WEB_PORT, 39999);

  //
  // User related properties
  //
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_THREADS =
      create(ConstantPropertyKey.Name.USER_BLOCK_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "8MB");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_BLOCK_REMOTE_READER_CLASS =
      create(ConstantPropertyKey.Name.USER_BLOCK_REMOTE_READER_CLASS,
          "alluxio.client.netty.NettyRemoteBlockReader");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_BLOCK_REMOTE_WRITER_CLASS =
      create(ConstantPropertyKey.Name.USER_BLOCK_REMOTE_WRITER_CLASS,
          "alluxio.client.netty.NettyRemoteBlockWriter");
  public static final PropertyKey USER_BLOCK_SIZE_BYTES_DEFAULT =
      create(ConstantPropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "512MB");
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_THREADS =
      create(ConstantPropertyKey.Name.USER_BLOCK_WORKER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX =
      create(ConstantPropertyKey.Name.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX, 128);
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      create(ConstantPropertyKey.Name.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS,
          300 * Constants.SECOND_MS);
  public static final PropertyKey USER_DATE_FORMAT_PATTERN =
      create(ConstantPropertyKey.Name.USER_DATE_FORMAT_PATTERN, "MM-dd-yyyy HH:mm:ss:SSS");
  public static final PropertyKey USER_FAILED_SPACE_REQUEST_LIMITS =
      create(ConstantPropertyKey.Name.USER_FAILED_SPACE_REQUEST_LIMITS, 3);
  public static final PropertyKey USER_FILE_BUFFER_BYTES =
      create(ConstantPropertyKey.Name.USER_FILE_BUFFER_BYTES, "1MB");
  public static final PropertyKey USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
      create(ConstantPropertyKey.Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, true);
  public static final PropertyKey USER_FILE_MASTER_CLIENT_THREADS =
      create(ConstantPropertyKey.Name.USER_FILE_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_FILE_PASSIVE_CACHE_ENABLED =
      create(ConstantPropertyKey.Name.USER_FILE_PASSIVE_CACHE_ENABLED, true);
  public static final PropertyKey USER_FILE_READ_TYPE_DEFAULT =
      create(ConstantPropertyKey.Name.USER_FILE_READ_TYPE_DEFAULT, "CACHE_PROMOTE");
  public static final PropertyKey USER_FILE_SEEK_BUFFER_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES, "1MB");
  public static final PropertyKey USER_FILE_WAITCOMPLETED_POLL_MS =
      create(ConstantPropertyKey.Name.USER_FILE_WAITCOMPLETED_POLL_MS, 1000);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_THREADS =
      create(ConstantPropertyKey.Name.USER_FILE_WORKER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX =
      create(ConstantPropertyKey.Name.USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX, 128);
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      create(ConstantPropertyKey.Name.USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS,
          300 * Constants.SECOND_MS);
  public static final PropertyKey USER_FILE_WRITE_LOCATION_POLICY =
      create(ConstantPropertyKey.Name.USER_FILE_WRITE_LOCATION_POLICY,
          "alluxio.client.file.policy.LocalFirstPolicy");
  public static final PropertyKey USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
      create(ConstantPropertyKey.Name.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES, "0MB");
  public static final PropertyKey USER_FILE_WRITE_TYPE_DEFAULT =
      create(ConstantPropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
  public static final PropertyKey USER_FILE_WRITE_TIER_DEFAULT =
      create(ConstantPropertyKey.Name.USER_FILE_WRITE_TIER_DEFAULT, Constants.FIRST_TIER);
  public static final PropertyKey USER_HEARTBEAT_INTERVAL_MS =
      create(ConstantPropertyKey.Name.USER_HEARTBEAT_INTERVAL_MS, 1000);
  public static final PropertyKey USER_HOSTNAME =
      create(ConstantPropertyKey.Name.USER_HOSTNAME, null);
  public static final PropertyKey USER_LINEAGE_ENABLED =
      create(ConstantPropertyKey.Name.USER_LINEAGE_ENABLED, false);
  public static final PropertyKey USER_LINEAGE_MASTER_CLIENT_THREADS =
      create(ConstantPropertyKey.Name.USER_LINEAGE_MASTER_CLIENT_THREADS, 10);
  public static final PropertyKey USER_LOCAL_READER_PACKET_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_LOCAL_READER_PACKET_SIZE_BYTES, "8MB");
  public static final PropertyKey USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_LOCAL_WRITER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_CHANNEL, null);
  public static final PropertyKey USER_NETWORK_NETTY_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_TIMEOUT_MS, 30000);
  public static final PropertyKey USER_NETWORK_NETTY_WORKER_THREADS =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_WORKER_THREADS, 0);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX, 1024);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS,
          300 * Constants.SECOND_MS);
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED, false);
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES, "64KB");
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS, 16);
  public static final PropertyKey USER_NETWORK_NETTY_READER_CANCEL_ENABLED =
      create(ConstantPropertyKey.Name.USER_NETWORK_NETTY_READER_CANCEL_ENABLED, true);
  public static final PropertyKey USER_PACKET_STREAMING_ENABLED =
      create(ConstantPropertyKey.Name.USER_PACKET_STREAMING_ENABLED, true);
  public static final PropertyKey USER_RPC_RETRY_BASE_SLEEP_MS =
      create(ConstantPropertyKey.Name.USER_RPC_RETRY_BASE_SLEEP_MS, 50);
  public static final PropertyKey USER_RPC_RETRY_MAX_NUM_RETRY =
      create(ConstantPropertyKey.Name.USER_RPC_RETRY_MAX_NUM_RETRY, 20);
  public static final PropertyKey USER_RPC_RETRY_MAX_SLEEP_MS =
      create(ConstantPropertyKey.Name.USER_RPC_RETRY_MAX_SLEEP_MS, 5000);
  // Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_ENABLED =
      create(ConstantPropertyKey.Name.USER_UFS_DELEGATION_ENABLED, true);
  // Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES, "8MB");
  public static final PropertyKey USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
      create(ConstantPropertyKey.Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES, "2MB");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_UFS_FILE_READER_CLASS =
      create(ConstantPropertyKey.Name.USER_UFS_FILE_READER_CLASS,
          "alluxio.client.netty.NettyUnderFileSystemFileReader");
  // Deprecated. It will be removed in 2.0.0.
  public static final PropertyKey USER_UFS_FILE_WRITER_CLASS =
      create(ConstantPropertyKey.Name.USER_UFS_FILE_WRITER_CLASS,
          "alluxio.client.netty.NettyUnderFileSystemFileWriter");
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY =
      create(ConstantPropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
          "alluxio.client.file.policy.LocalFirstPolicy");
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
      create(ConstantPropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS,
          1);
  public static final PropertyKey USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
      create(ConstantPropertyKey.Name.USER_UFS_BLOCK_READ_CONCURRENCY_MAX, Integer.MAX_VALUE);
  public static final PropertyKey USER_UFS_BLOCK_OPEN_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.USER_UFS_BLOCK_OPEN_TIMEOUT_MS, 300000);
  public static final PropertyKey USER_SHORT_CIRCUIT_ENABLED =
      create(ConstantPropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, true);

  //
  // FUSE integration related properties
  //
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  public static final PropertyKey FUSE_CACHED_PATHS_MAX =
      create(ConstantPropertyKey.Name.FUSE_CACHED_PATHS_MAX, 500);
  /** Have the fuse process log every FS request. */
  public static final PropertyKey FUSE_DEBUG_ENABLED =
      create(ConstantPropertyKey.Name.FUSE_DEBUG_ENABLED, false);

  /** FUSE file system name. */
  public static final PropertyKey FUSE_FS_NAME =
      create(ConstantPropertyKey.Name.FUSE_FS_NAME, "alluxio-fuse");
  public static final PropertyKey FUSE_FS_ROOT = create(ConstantPropertyKey.Name.FUSE_FS_ROOT, "/");
  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  public static final PropertyKey FUSE_MAXWRITE_BYTES =
      create(ConstantPropertyKey.Name.FUSE_MAXWRITE_BYTES, 131072);
  public static final PropertyKey FUSE_MOUNT_DEFAULT =
      create(ConstantPropertyKey.Name.FUSE_MOUNT_DEFAULT, "/mnt/alluxio");

  //
  // Security related properties
  //
  public static final PropertyKey SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
      create(ConstantPropertyKey.Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS, null);
  public static final PropertyKey SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS, "600000");
  public static final PropertyKey SECURITY_AUTHENTICATION_TYPE =
      create(ConstantPropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      create(ConstantPropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true);
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
      create(ConstantPropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "supergroup");
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_UMASK =
      create(ConstantPropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "022");
  public static final PropertyKey SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
      create(ConstantPropertyKey.Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS, "60000");
  public static final PropertyKey SECURITY_GROUP_MAPPING_CLASS =
      create(ConstantPropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.ShellBasedUnixGroupsMapping");
  public static final PropertyKey SECURITY_LOGIN_USERNAME =
      create(ConstantPropertyKey.Name.SECURITY_LOGIN_USERNAME, null);

  //
  // Mesos and Yarn related properties
  //
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_CPU =
      create(ConstantPropertyKey.Name.INTEGRATION_MASTER_RESOURCE_CPU, 1);
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_MEM =
      create(ConstantPropertyKey.Name.INTEGRATION_MASTER_RESOURCE_MEM, "1024MB");
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_JAR_URL =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_ALLUXIO_JAR_URL, String.format(
          "http://downloads.alluxio.org/downloads/files/${%s}/" + "alluxio-${%s}-bin.tar.gz",
          ConstantPropertyKey.Name.VERSION, ConstantPropertyKey.Name.VERSION));
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NAME =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME, "AlluxioMaster");
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT, 1);
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_WORKER_NAME =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME, "AlluxioWorker");
  public static final PropertyKey INTEGRATION_MESOS_JDK_PATH =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_JDK_PATH, "jdk1.7.0_79");
  public static final PropertyKey INTEGRATION_MESOS_JDK_URL =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_JDK_URL,
          "https://alluxio-mesos.s3.amazonaws.com/jdk-7u79-linux-x64.tar.gz");
  public static final PropertyKey INTEGRATION_MESOS_PRINCIPAL =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_PRINCIPAL, "alluxio");
  public static final PropertyKey INTEGRATION_MESOS_ROLE =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_ROLE, "*");
  public static final PropertyKey INTEGRATION_MESOS_SECRET =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_SECRET, null);
  public static final PropertyKey INTEGRATION_MESOS_USER =
      create(ConstantPropertyKey.Name.INTEGRATION_MESOS_USER, "");
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_CPU =
      create(ConstantPropertyKey.Name.INTEGRATION_WORKER_RESOURCE_CPU, 1);
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_MEM =
      create(ConstantPropertyKey.Name.INTEGRATION_WORKER_RESOURCE_MEM, "1024MB");
  public static final PropertyKey INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
      create(ConstantPropertyKey.Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX, 1);

  /** A map from default property key's string name to the key. */
  private static final Map<PropertyKey, Object> DEFAULT_VALUES = new HashMap<>();

  /**
   * @param keyStr string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String keyStr) {
    return ConstantPropertyKey.isValid(keyStr) || ParameterizedPropertyKey.isValid(keyStr);
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param keyStr string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String keyStr) {
    PropertyKey key = ConstantPropertyKey.fromString(keyStr);
    if (key != null) {
      return key;
    }
    key = ParameterizedPropertyKey.fromString(keyStr);
    if (key != null) {
      return key;
    }
    throw new IllegalArgumentException(
        ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(keyStr));
  }

  /**
   * @return all pre-defined property keys
   */
  public static Collection<? extends PropertyKey> getDefaultKeys() {
    return ConstantPropertyKey.getDefaultKeys();
  }

  /** Property name. */
  private final String mName;

  /**
   * @param name String of this property
   */
  PropertyKey(String name) {
    mName = Preconditions.checkNotNull(name, "name");
  }

  /**
   * Factory method to create a constant default property and assign a default value.
   *
   * @param name String of this property
   * @param defaultValue Default value of this property in compile time if not null
   */
  static PropertyKey create(String name, Object defaultValue) {
    ConstantPropertyKey key = new ConstantPropertyKey(name);
    DEFAULT_VALUES.put(key, defaultValue);
    return key;
  }

  /**
   * Factory method to create a property from template and assign a default value.
   *
   * @param template String of this property
   * @param defaultValue Default value of this property in compile time if not null
   */
  static PropertyKey create(ParameterizedPropertyKey.Template template,
      Object defaultValue, Object... param) {
    ParameterizedPropertyKey key = template.format(param);
    DEFAULT_VALUES.put(key, defaultValue);
    return key;
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
   * @return length of this property key
   */
  public int length() {
    return mName.length();
  }

  /**
   * @param key the name of input key
   * @return if this key is nested inside the given key
   */
  public boolean isNested(String key) {
    return key.length() > length() + 1 && key.startsWith(mName) && key.charAt(length()) == '.';
  }

  /**
   * @return the default value of a property key or null if no default value set
   */
  public String getDefaultValue() {
    Object value = DEFAULT_VALUES.get(this);
    return value != null ? value.toString() : null;
  }
}
