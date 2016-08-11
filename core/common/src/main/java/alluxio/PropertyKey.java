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

/**
 * Configurations properties constants. Please check and update Configuration-Settings.md file when
 * you change or add Alluxio configuration properties.
 */
public enum PropertyKey {
  HOME("alluxio.home"),
  DEBUG("alluxio.debug"),
  LOGGER_TYPE("alluxio.logger.type"),
  VERSION("alluxio.version"),
  WEB_RESOURCES("alluxio.web.resources"),
  WEB_THREADS("alluxio.web.threads"),
  LOGS_DIR("alluxio.logs.dir"),
  SITE_CONF_DIR("alluxio.site.conf.dir"),
  TEST_MODE("alluxio.test.mode"),
  ZOOKEEPER_ENABLED("alluxio.zookeeper.enabled"),
  ZOOKEEPER_ADDRESS("alluxio.zookeeper.address"),
  ZOOKEEPER_ELECTION_PATH("alluxio.zookeeper.election.path"),
  ZOOKEEPER_LEADER_PATH("alluxio.zookeeper.leader.path"),
  ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT("alluxio.zookeeper.leader.inquiry.retry"),
  KEY_VALUE_ENABLED("alluxio.keyvalue.enabled"),
  KEY_VALUE_PARTITION_SIZE_BYTES_MAX("alluxio.keyvalue.partition.size.bytes.max"),
  METRICS_CONF_FILE("alluxio.metrics.conf.file"),
  INTEGRATION_MASTER_RESOURCE_CPU("alluxio.integration.master.resource.cpu"),
  INTEGRATION_MASTER_RESOURCE_MEM("alluxio.integration.master.resource.mem"),
  INTEGRATION_YARN_WORKERS_PER_HOST_MAX("alluxio.integration.yarn.workers.per.host.max"),
  INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH("alluxio.integration.mesos.executor.dependency.path"),
  INTEGRATION_MESOS_JRE_PATH("alluxio.integration.mesos.jre.path"),
  INTEGRATION_MESOS_JRE_URL("alluxio.integration.mesos.jre.url"),
  INTEGRATION_MESOS_PRINCIPAL("alluxio.integration.mesos.principal"),
  INTEGRATION_MESOS_ROLE("alluxio.integration.mesos.role"),
  INTEGRATION_MESOS_SECRET("alluxio.integration.mesos.secret"),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NAME("alluxio.integration.mesos.master.name"),
  INTEGRATION_MESOS_ALLUXIO_WORKER_NAME("alluxio.integration.mesos.worker.name"),
  INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT("alluxio.integration.mesos.master.node.count"),
  INTEGRATION_MESOS_USER("alluxio.integration.mesos.user"),
  INTEGRATION_WORKER_RESOURCE_CPU("alluxio.integration.worker.resource.cpu"),
  INTEGRATION_WORKER_RESOURCE_MEM("alluxio.integration.worker.resource.mem"),

  //
  // UFS related properties
  //
  UNDERFS_ADDRESS("alluxio.underfs.address"),
  UNDERFS_HDFS_IMPL("alluxio.underfs.hdfs.impl"),
  UNDERFS_HDFS_CONFIGURATION("alluxio.underfs.hdfs.configuration"),
  UNDERFS_HDFS_PREFIXES("alluxio.underfs.hdfs.prefixes"),
  UNDERFS_GLUSTERFS_IMPL("alluxio.underfs.glusterfs.impl"),
  UNDERFS_GLUSTERFS_VOLUMES("alluxio.underfs.glusterfs.volumes"),
  UNDERFS_GLUSTERFS_MOUNTS("alluxio.underfs.glusterfs.mounts"),
  UNDERFS_GLUSTERFS_MR_DIR("alluxio.underfs.glusterfs.mapred.system.dir"),
  UNDERFS_OSS_CONNECT_MAX("alluxio.underfs.oss.connection.max"),
  UNDERFS_OSS_CONNECT_TIMEOUT("alluxio.underfs.oss.connection.timeout.ms"),
  UNDERFS_OSS_CONNECT_TTL("alluxio.underfs.oss.connection.ttl"),
  UNDERFS_OSS_SOCKET_TIMEOUT("alluxio.underfs.oss.socket.timeout.ms"),
  UNDERFS_S3_PROXY_HOST("alluxio.underfs.s3.proxy.host"),
  UNDERFS_S3_PROXY_PORT("alluxio.underfs.s3.proxy.port"),
  UNDERFS_S3_PROXY_HTTPS_ONLY("alluxio.underfs.s3.proxy.https.only"),
  UNDERFS_S3_ENDPOINT("alluxio.underfs.s3.endpoint"),
  UNDERFS_S3_ENDPOINT_HTTP_PORT("alluxio.underfs.s3.endpoint.http.port"),
  UNDERFS_S3_ENDPOINT_HTTPS_PORT("alluxio.underfs.s3.endpoint.https.port"),
  UNDERFS_S3_DISABLE_DNS_BUCKETS("alluxio.underfs.s3.disable.dns.buckets"),
  UNDERFS_S3_THREADS_MAX("alluxio.underfs.s3.threads.max"),
  UNDERFS_S3_ADMIN_THREADS_MAX("alluxio.underfs.s3.admin.threads.max"),
  UNDERFS_S3_UPLOAD_THREADS_MAX("alluxio.underfs.s3.upload.threads.max"),
  UNDERFS_S3A_SECURE_HTTP_ENABLED("alluxio.underfs.s3a.secure.http.enabled"),
  UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED("alluxio.underfs.s3a.server.side.encryption.enabled"),
  UNDERFS_S3A_SOCKET_TIMEOUT_MS("alluxio.underfs.s3a.socket.timeout.ms"),

  //
  // Master related properties
  //
  MASTER_JOURNAL_FLUSH_BATCH_TIME_MS("alluxio.master.journal.flush.batch.time.ms"),
  MASTER_JOURNAL_FOLDER("alluxio.master.journal.folder"),
  MASTER_JOURNAL_FORMATTER_CLASS("alluxio.master.journal.formatter.class"),
  MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS(
      "alluxio.master.journal.tailer.shutdown.quiet.wait.time.ms"),
  MASTER_JOURNAL_TAILER_SLEEP_TIME_MS("alluxio.master.journal.tailer.sleep.time.ms"),
  MASTER_JOURNAL_LOG_SIZE_BYTES_MAX("alluxio.master.journal.log.size.bytes.max"),
  MASTER_FILE_ASYNC_PERSIST_HANDLER("alluxio.master.file.async.persist.handler"),
  MASTER_HOSTNAME("alluxio.master.hostname"),
  MASTER_BIND_HOST("alluxio.master.bind.host"),
  MASTER_RPC_PORT("alluxio.master.port"),
  MASTER_ADDRESS("alluxio.master.address"),
  MASTER_WEB_HOSTNAME("alluxio.master.web.hostname"),
  MASTER_WEB_BIND_HOST("alluxio.master.web.bind.host"),
  MASTER_WEB_PORT("alluxio.master.web.port"),
  MASTER_HEARTBEAT_INTERVAL_MS("alluxio.master.heartbeat.interval.ms"),
  MASTER_TTL_CHECKER_INTERVAL_MS("alluxio.master.ttl.checker.interval.ms"),
  MASTER_WORKER_THREADS_MAX("alluxio.master.worker.threads.max"),
  MASTER_WORKER_THREADS_MIN("alluxio.master.worker.threads.min"),
  MASTER_WORKER_TIMEOUT_MS("alluxio.master.worker.timeout.ms"),
  MASTER_WHITELIST("alluxio.master.whitelist"),
  MASTER_KEYTAB_KEY_FILE("alluxio.master.keytab.file"),
  MASTER_PRINCIPAL("alluxio.master.principal"),
  MASTER_RETRY("alluxio.master.retry"),
  MASTER_LINEAGE_CHECKPOINT_CLASS("alluxio.master.lineage.checkpoint.class"),
  MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS("alluxio.master.lineage.checkpoint.interval.ms"),
  MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS("alluxio.master.lineage.recompute.interval.ms"),
  MASTER_LINEAGE_RECOMPUTE_LOG_PATH("alluxio.master.lineage.recompute.log.path"),
  MASTER_TIERED_STORE_GLOBAL_LEVELS("alluxio.master.tieredstore.global.levels"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS("alluxio.master.tieredstore.global.level0.alias"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS("alluxio.master.tieredstore.global.level1.alias"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS("alluxio.master.tieredstore.global.level2.alias"),
  MASTER_FORMAT_FILE_PREFIX("alluxio.master.format.file_prefix"),

  NETWORK_HOST_RESOLUTION_TIMEOUT_MS("alluxio.network.host.resolution.timeout.ms"),
  NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX("alluxio.network.thrift.frame.size.bytes.max"),

  //
  // Worker related properties
  //
  WORKER_MEMORY_SIZE("alluxio.worker.memory.size"),
  WORKER_HOSTNAME("alluxio.worker.hostname"),
  WORKER_BIND_HOST("alluxio.worker.bind.host"),
  WORKER_RPC_PORT("alluxio.worker.port"),
  WORKER_DATA_HOSTNAME("alluxio.worker.data.hostname"),
  WORKER_DATA_BIND_HOST("alluxio.worker.data.bind.host"),
  WORKER_DATA_PORT("alluxio.worker.data.port"),
  WORKER_WEB_HOSTNAME("alluxio.worker.web.hostname"),
  WORKER_WEB_BIND_HOST("alluxio.worker.web.bind.host"),
  WORKER_WEB_PORT("alluxio.worker.web.port"),
  WORKER_DATA_FOLDER("alluxio.worker.data.folder"),
  WORKER_DATA_TMP_FOLDER("alluxio.worker.data.folder.tmp"),
  WORKER_DATA_TMP_SUBDIR_MAX("alluxio.worker.data.tmp.subdir.max"),
  WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS("alluxio.worker.block.heartbeat.timeout.ms"),
  WORKER_BLOCK_HEARTBEAT_INTERVAL_MS("alluxio.worker.block.heartbeat.interval.ms"),
  WORKER_SESSION_TIMEOUT_MS("alluxio.worker.session.timeout.ms"),
  WORKER_WORKER_BLOCK_THREADS_MAX("alluxio.worker.block.threads.max"),
  WORKER_WORKER_BLOCK_THREADS_MIN("alluxio.worker.block.threads.min"),
  WORKER_NETWORK_NETTY_BOSS_THREADS("alluxio.worker.network.netty.boss.threads"),
  WORKER_NETWORK_NETTY_WORKER_THREADS("alluxio.worker.network.netty.worker.threads"),
  WORKER_NETWORK_NETTY_CHANNEL("alluxio.worker.network.netty.channel"),
  WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE("alluxio.worker.network.netty.file.transfer"),
  WORKER_NETWORK_NETTY_WATERMARK_HIGH("alluxio.worker.network.netty.watermark.high"),
  WORKER_NETWORK_NETTY_WATERMARK_LOW("alluxio.worker.network.netty.watermark.low"),
  WORKER_NETWORK_NETTY_BACKLOG("alluxio.worker.network.netty.backlog"),
  WORKER_NETWORK_NETTY_BUFFER_SEND("alluxio.worker.network.netty.buffer.send"),
  WORKER_NETWORK_NETTY_BUFFER_RECEIVE("alluxio.worker.network.netty.buffer.receive"),
  WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD("alluxio.worker.network.netty.shutdown.quiet.period"),
  WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT("alluxio.worker.network.netty.shutdown.timeout"),
  WORKER_ALLOCATOR_CLASS("alluxio.worker.allocator.class"),
  WORKER_EVICTOR_CLASS("alluxio.worker.evictor.class"),
  WORKER_EVICTOR_LRFU_STEP_FACTOR("alluxio.worker.evictor.lrfu.step.factor"),
  WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR("alluxio.worker.evictor.lrfu.attenuation.factor"),
  WORKER_TIERED_STORE_LEVELS("alluxio.worker.tieredstore.levels"),
  WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS("alluxio.worker.filesystem.heartbeat.interval.ms"),
  WORKER_FILE_PERSIST_POOL_SIZE("alluxio.worker.file.persist.pool.size"),
  WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED("alluxio.worker.file.persist.rate.limit.enabled"),
  WORKER_FILE_PERSIST_RATE_LIMIT("alluxio.worker.file.persist.rate.limit"),
  WORKER_TIERED_STORE_BLOCK_LOCKS("alluxio.worker.tieredstore.block.locks"),

  WORKER_TIERED_STORE_LEVEL0_ALIAS("alluxio.worker.tieredstore.level0.alias"),
  WORKER_TIERED_STORE_LEVEL0_DIRS_PATH("alluxio.worker.tieredstore.level0.dirs.path"),
  WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA("alluxio.worker.tieredstore.level0.dirs.quota"),
  WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO("alluxio.worker.tieredstore.level0.reserved.ratio"),

  WORKER_TIERED_STORE_LEVEL1_ALIAS("alluxio.worker.tieredstore.level1.alias"),
  WORKER_TIERED_STORE_LEVEL1_DIRS_PATH("alluxio.worker.tieredstore.level1.dirs.path"),
  WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA("alluxio.worker.tieredstore.level1.dirs.quota"),
  WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO("alluxio.worker.tieredstore.level1.reserved.ratio"),

  WORKER_TIERED_STORE_LEVEL2_ALIAS("alluxio.worker.tieredstore.level2.alias"),
  WORKER_TIERED_STORE_LEVEL2_DIRS_PATH("alluxio.worker.tieredstore.level2.dirs.path"),
  WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA("alluxio.worker.tieredstore.level2.dirs.quota"),
  WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO("alluxio.worker.tieredstore.level2.reserved.ratio"),

  WORKER_TIERED_STORE_RESERVER_INTERVAL_MS("alluxio.worker.tieredstore.reserver.interval.ms"),
  WORKER_TIERED_STORE_RESERVER_ENABLED("alluxio.worker.tieredstore.reserver.enabled"),
  WORKER_KEYTAB_FILE("alluxio.worker.keytab.file"),
  WORKER_PRINCIPAL("alluxio.worker.principal"),
  WORKER_DATA_SERVER_CLASS("alluxio.worker.data.server.class"),

  //
  // User related properties
  //
  USER_FAILED_SPACE_REQUEST_LIMITS("alluxio.user.failed.space.request.limits"),
  USER_FILE_BUFFER_BYTES("alluxio.user.file.buffer.bytes"),
  USER_HEARTBEAT_INTERVAL_MS("alluxio.user.heartbeat.interval.ms"),
  USER_BLOCK_SIZE_BYTES_DEFAULT("alluxio.user.block.size.bytes.default"),
  USER_NETWORK_NETTY_WORKER_THREADS("alluxio.user.network.netty.worker.threads"),
  USER_NETWORK_NETTY_CHANNEL("alluxio.user.network.netty.channel"),
  USER_NETWORK_NETTY_TIMEOUT_MS("alluxio.user.network.netty.timeout.ms"),
  USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES("alluxio.user.block.remote.read.buffer.size.bytes"),
  USER_FILE_WRITE_TYPE_DEFAULT("alluxio.user.file.writetype.default"),
  USER_FILE_READ_TYPE_DEFAULT("alluxio.user.file.readtype.default"),
  USER_FILE_WRITE_LOCATION_POLICY("alluxio.user.file.write.location.policy.class"),
  USER_FILE_CACHE_PARTIALLY_READ_BLOCK("alluxio.user.file.cache.partially.read.block"),
  USER_FILE_SEEK_BUFFER_SIZE_BYTES("alluxio.user.file.seek.buffer.size.bytes"),
  USER_BLOCK_REMOTE_READER("alluxio.user.block.remote.reader.class"),
  USER_BLOCK_REMOTE_WRITER("alluxio.user.block.remote.writer.class"),
  USER_BLOCK_WORKER_CLIENT_THREADS("alluxio.user.block.worker.client.threads"),
  USER_BLOCK_MASTER_CLIENT_THREADS("alluxio.user.block.master.client.threads"),
  USER_FILE_WORKER_CLIENT_THREADS("alluxio.user.file.worker.client.threads"),
  USER_FILE_MASTER_CLIENT_THREADS("alluxio.user.file.master.client.threads"),
  USER_LINEAGE_MASTER_CLIENT_THREADS("alluxio.user.lineage.master.client.threads"),
  USER_LINEAGE_ENABLED("alluxio.user.lineage.enabled"),
  USER_FILE_WAITCOMPLETED_POLL_MS("alluxio.user.file.waitcompleted.poll.ms"),
  USER_UFS_DELEGATION_ENABLED("alluxio.user.ufs.delegation.enabled"),
  USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES("alluxio.user.ufs.delegation.read.buffer.size.bytes"),
  USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES(
      "alluxio.user.ufs.delegation.write.buffer.size.bytes"),

  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  FUSE_MAXWRITE_BYTES("alluxio.fuse.maxwrite.bytes"),
  /** Have the fuse process log every FS request. */
  FUSE_DEBUG_ENABLED("alluxio.fuse.debug.enabled"),
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  FUSE_CACHED_PATHS_MAX("alluxio.fuse.cached.paths.max"),
  FUSE_MOUNT_DEFAULT("alluxio.fuse.mount.default"),
  FUSE_FS_ROOT("alluxio.fuse.fs.root"),
  /** FUSE file system name. */
  FUSE_FS_NAME("alluxio.fuse.fs.name"),
  OSS_ACCESS_KEY("fs.oss.accessKeyId"),
  OSS_SECRET_KEY("fs.oss.accessKeySecret"),
  OSS_ENDPOINT_KEY("fs.oss.endpoint"),
  S3N_ACCESS_KEY("fs.s3n.awsAccessKeyId"),
  S3N_SECRET_KEY("fs.s3n.awsSecretAccessKey"),
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  S3A_ACCESS_KEY("aws.accessKeyId"),
  S3A_SECRET_KEY("aws.secretKey"),
  GCS_ACCESS_KEY("fs.gcs.accessKeyId"),
  GCS_SECRET_KEY("fs.gcs.secretAccessKey"),
  SWIFT_USER_KEY("fs.swift.user"),
  SWIFT_TENANT_KEY("fs.swift.tenant"),
  SWIFT_API_KEY("fs.swift.apikey"),
  SWIFT_PASSWORD_KEY("fs.swift.password"),
  SWIFT_AUTH_URL_KEY("fs.swift.auth.url"),
  SWIFT_AUTH_PORT_KEY("fs.swift.auth.port"),
  SWIFT_AUTH_METHOD_KEY("fs.swift.auth.method"),
  SWIFT_USE_PUBLIC_URI_KEY("fs.swift.use.public.url"),
  SWIFT_SIMULATION("fs.swift.simulation"),

  //
  // Security related properties
  //
  // Authentication
  SECURITY_AUTHENTICATION_TYPE("alluxio.security.authentication.type"),
  SECURITY_AUTHENTICATION_CUSTOM_PROVIDER("alluxio.security.authentication.custom.provider.class"),
  SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS("alluxio.security.authentication.socket.timeout.ms"),
  SECURITY_LOGIN_USERNAME("alluxio.security.login.username"),
  // Authorization
  SECURITY_AUTHORIZATION_PERMISSION_ENABLED("alluxio.security.authorization.permission.enabled"),
  SECURITY_AUTHORIZATION_PERMISSION_UMASK("alluxio.security.authorization.permission.umask"),
  SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP(
      "alluxio.security.authorization.permission.supergroup"),
  // Group Mapping
  SECURITY_GROUP_MAPPING_CLASS("alluxio.security.group.mapping.class"),
  SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS("alluxio.security.group.mapping.cache.timeout.ms"),
  ;

  //
  // A set of format templates to generate the property name for a given level in tiered storage.
  // e.g.,
  // {@code PropertyKey.format(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 0)}
  //
  public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT =
      "alluxio.master.tieredstore.global.level%d.alias";
  public static final String WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT =
      "alluxio.worker.tieredstore.level%d.alias";
  public static final String WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT =
      "alluxio.worker.tieredstore.level%d.dirs.path";
  public static final String WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT =
      "alluxio.worker.tieredstore.level%d.dirs.quota";
  public static final String WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT =
      "alluxio.worker.tieredstore.level%d.reserved.ratio";

  private final String mString;

  /**
   * @param keyStr string of property key
   * @return whether a string parsable as a property key name
   */
  public static boolean isValid(String keyStr) {
    for (PropertyKey key : PropertyKey.values()) {
      if (key.toString().equals(keyStr)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param keyStr string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String keyStr) {
    for (PropertyKey key : PropertyKey.values()) {
      if (key.toString().equals(keyStr)) {
        return key;
      }
    }
    throw new IllegalArgumentException("Invalid property key " + keyStr);
  }

  /**
   * Converts a property key template (e.g.,
   * {@link PropertyKey#WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT})
   * to a {@link PropertyKey} enum instance.
   *
   * @param format string format template
   * @param ordinal oridnal
   * @return corresponding property
   */
  public static PropertyKey format(String format, int ordinal) {
    return PropertyKey.fromString(String.format(format, ordinal));
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
