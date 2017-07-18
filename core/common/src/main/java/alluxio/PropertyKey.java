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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration property keys. This class provides a set of pre-defined property keys.
 */
@ThreadSafe
public class PropertyKey {
  // The following two maps must be the first to initialize within this file.
  /** A map from default property key's string name to the key. */
  private static final Map<String, PropertyKey> DEFAULT_KEYS_MAP = new HashMap<>();
  /** A map from default property key's string name to the key. */
  private static final Map<PropertyKey, Object> DEFAULT_VALUES = new HashMap<>();

  /**
   * Builder to create {@link PropertyKey} instances.
   */
  public static final class Builder {
    private String[] mAlias;
    private Object mDefaultValue;
    private String mDescription;
    private final String mName;

    /**
     * @param name name of this property to build
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param template template of the name of the property to build
     * @param params parameters of the template
     */
    public Builder(PropertyKey.Template template, Object... params) {
      mName = String.format(template.mFormat, params);
    }

    /**
     * @param alias alias of this property key to build
     * @return the updated builder instance
     */
    public Builder setAlias(String[] alias) {
      mAlias = Arrays.copyOf(alias, alias.length);
      return this;
    }

    /**
     * @param defaultValue default value of this property key to build
     * @return the updated builder instance
     */
    public Builder setDefaultValue(Object defaultValue) {
      mDefaultValue = defaultValue;
      return this;
    }

    /**
     * @param description of this property key to build
     * @return the updated builder instance
     */
    public Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * @return the created property key instance
     */
    public PropertyKey build() {
      return PropertyKey.create(mName, mDefaultValue, mAlias);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("alias", mAlias)
          .add("defaultValue", mDefaultValue)
          .add("description", mDescription)
          .add("name", mName).toString();
    }
  }

  public static final PropertyKey CONF_DIR =
      new Builder(Name.CONF_DIR)
          .setDefaultValue(String.format("${%s}/conf", Name.HOME))
          .build();
  public static final PropertyKey DEBUG =
      new Builder(Name.DEBUG)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey HOME =
      new Builder(Name.HOME)
          .setDefaultValue("/opt/alluxio")
          .build();
  public static final PropertyKey KEY_VALUE_ENABLED =
      new Builder(Name.KEY_VALUE_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
      new Builder(Name.KEY_VALUE_PARTITION_SIZE_BYTES_MAX)
          .setDefaultValue("512MB")
          .build();
  public static final PropertyKey LOGGER_TYPE =
      new Builder(Name.LOGGER_TYPE)
          .setDefaultValue("Console")
          .build();
  public static final PropertyKey LOGS_DIR =
      new Builder(Name.LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", Name.WORK_DIR))
          .build();
  public static final PropertyKey METRICS_CONF_FILE =
      new Builder(Name.METRICS_CONF_FILE)
          .setDefaultValue(String.format("${%s}/metrics.properties", Name.CONF_DIR))
          .build();
  public static final PropertyKey NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      new Builder(Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)
          .setDefaultValue("5sec")
          .build();
  public static final PropertyKey NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS =
      new Builder(Name.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS)
          .setDefaultValue("30sec")
          .build();
  public static final PropertyKey NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
      new Builder(Name.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX)
          .setDefaultValue("16MB")
          .build();
  public static final PropertyKey SITE_CONF_DIR =
      new Builder(Name.SITE_CONF_DIR)
          .setDefaultValue("${user.home}/.alluxio/,/etc/alluxio/")
          .build();
  public static final PropertyKey TEST_MODE =
      new Builder(Name.TEST_MODE)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey VERSION =
      new Builder(Name.VERSION)
          .setDefaultValue(ProjectConstants.VERSION)
          .build();
  public static final PropertyKey WEB_RESOURCES =
      new Builder(Name.WEB_RESOURCES)
          .setDefaultValue(String.format("${%s}/core/server/common/src/main/webapp", Name.HOME))
          .build();
  public static final PropertyKey WEB_THREADS =
      new Builder(Name.WEB_THREADS)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey WORK_DIR =
      new Builder(Name.WORK_DIR)
          .setDefaultValue(String.format("${%s}", Name.HOME))
          .build();
  public static final PropertyKey ZOOKEEPER_ADDRESS =
      new Builder(Name.ZOOKEEPER_ADDRESS)
          .build();
  public static final PropertyKey ZOOKEEPER_ELECTION_PATH =
      new Builder(Name.ZOOKEEPER_ELECTION_PATH)
          .setDefaultValue("/election")
          .build();
  public static final PropertyKey ZOOKEEPER_ENABLED =
      new Builder(Name.ZOOKEEPER_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      new Builder(Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_PATH =
      new Builder(Name.ZOOKEEPER_LEADER_PATH)
          .setDefaultValue("/leader")
          .build();

  /**
   * UFS related properties.
   *
   * @deprecated since 1.5.0 and will be removed in 2.0. Use MASTER_MOUNT_TABLE_ROOT_UFS instead.
   */
  @Deprecated
  public static final PropertyKey UNDERFS_ADDRESS =
      new Builder(Name.UNDERFS_ADDRESS)
          .setDefaultValue(String.format("${%s}/underFSStorage", Name.WORK_DIR))
          .build();
  public static final PropertyKey UNDERFS_ALLOW_SET_OWNER_FAILURE =
      new Builder(Name.UNDERFS_ALLOW_SET_OWNER_FAILURE)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_LISTING_LENGTH =
      new Builder(Name.UNDERFS_LISTING_LENGTH)
          .setDefaultValue(1000)
          .build();
  public static final PropertyKey UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
      new Builder(Name.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING)
          .setDefaultValue("")
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_IMPL =
      new Builder(Name.UNDERFS_GLUSTERFS_IMPL)
          .setDefaultValue("org.apache.hadoop.fs.glusterfs.GlusterFileSystem")
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_MOUNTS =
      new Builder(Name.UNDERFS_GLUSTERFS_MOUNTS)
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_MR_DIR =
      new Builder(Name.UNDERFS_GLUSTERFS_MR_DIR)
          .setDefaultValue("glusterfs:///mapred/system")
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_VOLUMES =
      new Builder(Name.UNDERFS_GLUSTERFS_VOLUMES)
          .build();
  public static final PropertyKey UNDERFS_HDFS_CONFIGURATION =
      new Builder(Name.UNDERFS_HDFS_CONFIGURATION)
          .setDefaultValue(String.format(
              "${%s}/core-site.xml:${%s}/hdfs-site.xml", Name.CONF_DIR, Name.CONF_DIR))
          .build();
  public static final PropertyKey UNDERFS_HDFS_IMPL =
      new Builder(Name.UNDERFS_HDFS_IMPL)
          .setDefaultValue("org.apache.hadoop.hdfs.DistributedFileSystem")
          .build();
  public static final PropertyKey UNDERFS_HDFS_PREFIXES =
      new Builder(Name.UNDERFS_HDFS_PREFIXES)
          .setDefaultValue("hdfs://,glusterfs:///,maprfs:///")
          .build();
  public static final PropertyKey UNDERFS_HDFS_REMOTE =
      new Builder(Name.UNDERFS_HDFS_REMOTE)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_SERVICE_THREADS =
      new Builder(Name.UNDERFS_OBJECT_STORE_SERVICE_THREADS)
          .setDefaultValue(20)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
      new Builder(Name.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_MAX =
      new Builder(Name.UNDERFS_OSS_CONNECT_MAX)
          .setDefaultValue(1024)
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_TIMEOUT =
      new Builder(Name.UNDERFS_OSS_CONNECT_TIMEOUT)
          .setDefaultValue("50sec")
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_TTL =
      new Builder(Name.UNDERFS_OSS_CONNECT_TTL)
          .setDefaultValue(-1)
          .build();
  public static final PropertyKey UNDERFS_OSS_SOCKET_TIMEOUT =
      new Builder(Name.UNDERFS_OSS_SOCKET_TIMEOUT)
          .setDefaultValue("50sec")
          .build();
  public static final PropertyKey UNDERFS_S3_ADMIN_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_ADMIN_THREADS_MAX)
          .setDefaultValue(20)
          .build();
  public static final PropertyKey UNDERFS_S3_DISABLE_DNS_BUCKETS =
      new Builder(Name.UNDERFS_S3_DISABLE_DNS_BUCKETS)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_S3_ENDPOINT =
      new Builder(Name.UNDERFS_S3_ENDPOINT)
          .build();
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTP_PORT =
      new Builder(Name.UNDERFS_S3_ENDPOINT_HTTP_PORT)
          .build();
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTPS_PORT =
      new Builder(Name.UNDERFS_S3_ENDPOINT_HTTPS_PORT)
          .build();
  public static final PropertyKey UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
      new Builder(Name.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING)
          .setDefaultValue("")
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_HOST =
      new Builder(Name.UNDERFS_S3_PROXY_HOST)
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_HTTPS_ONLY =
      new Builder(Name.UNDERFS_S3_PROXY_HTTPS_ONLY)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_PORT =
      new Builder(Name.UNDERFS_S3_PROXY_PORT)
          .build();
  public static final PropertyKey UNDERFS_S3_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_THREADS_MAX)
          .setDefaultValue(40)
          .build();
  public static final PropertyKey UNDERFS_S3_UPLOAD_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_UPLOAD_THREADS_MAX)
          .setDefaultValue(20)
          .build();
  public static final PropertyKey UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
      new Builder(Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS)
          .setDefaultValue("1min")
          .build();
  public static final PropertyKey UNDERFS_S3A_DIRECTORY_SUFFIX =
      new Builder(Name.UNDERFS_S3A_DIRECTORY_SUFFIX)
          .setDefaultValue("/")
          .build();
  public static final PropertyKey UNDERFS_S3A_INHERIT_ACL =
      new Builder(Name.UNDERFS_S3A_INHERIT_ACL)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey UNDERFS_S3A_LIST_OBJECTS_VERSION_1 =
      new Builder(Name.UNDERFS_S3A_LIST_OBJECTS_VERSION_1)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_S3A_REQUEST_TIMEOUT =
      new Builder(Name.UNDERFS_S3A_REQUEST_TIMEOUT_MS)
          .setDefaultValue("1min")
          .build();
  public static final PropertyKey UNDERFS_S3A_SECURE_HTTP_ENABLED =
      new Builder(Name.UNDERFS_S3A_SECURE_HTTP_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
      new Builder(Name.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey UNDERFS_S3A_SIGNER_ALGORITHM =
      new Builder(Name.UNDERFS_S3A_SIGNER_ALGORITHM)
          .build();
  public static final PropertyKey UNDERFS_S3A_SOCKET_TIMEOUT_MS =
      new Builder(Name.UNDERFS_S3A_SOCKET_TIMEOUT_MS)
          .setDefaultValue("50sec")
          .build();

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  public static final PropertyKey GCS_ACCESS_KEY = new Builder(Name.GCS_ACCESS_KEY).build();
  public static final PropertyKey GCS_SECRET_KEY = new Builder(Name.GCS_SECRET_KEY).build();
  public static final PropertyKey OSS_ACCESS_KEY = new Builder(Name.OSS_ACCESS_KEY).build();
  public static final PropertyKey OSS_ENDPOINT_KEY = new Builder(Name.OSS_ENDPOINT_KEY).build();
  public static final PropertyKey OSS_SECRET_KEY = new Builder(Name.OSS_SECRET_KEY).build();
  public static final PropertyKey S3A_ACCESS_KEY = new Builder(Name.S3A_ACCESS_KEY).build();
  public static final PropertyKey S3A_SECRET_KEY = new Builder(Name.S3A_SECRET_KEY).build();
  public static final PropertyKey S3N_ACCESS_KEY = new Builder(Name.S3N_ACCESS_KEY).build();
  public static final PropertyKey S3N_SECRET_KEY = new Builder(Name.S3N_SECRET_KEY).build();
  public static final PropertyKey SWIFT_API_KEY = new Builder(Name.SWIFT_API_KEY).build();
  public static final PropertyKey SWIFT_AUTH_METHOD_KEY =
      new Builder(Name.SWIFT_AUTH_METHOD_KEY).build();
  public static final PropertyKey SWIFT_AUTH_URL_KEY = new Builder(Name.SWIFT_AUTH_URL_KEY).build();
  public static final PropertyKey SWIFT_PASSWORD_KEY = new Builder(Name.SWIFT_PASSWORD_KEY).build();
  public static final PropertyKey SWIFT_SIMULATION = new Builder(Name.SWIFT_SIMULATION).build();
  public static final PropertyKey SWIFT_TENANT_KEY = new Builder(Name.SWIFT_TENANT_KEY).build();
  public static final PropertyKey SWIFT_USE_PUBLIC_URI_KEY =
      new Builder(Name.SWIFT_USE_PUBLIC_URI_KEY).build();
  public static final PropertyKey SWIFT_USER_KEY = new Builder(Name.SWIFT_USER_KEY).build();
  public static final PropertyKey SWIFT_REGION_KEY = new Builder(Name.SWIFT_REGION_KEY).build();

  // Journal ufs related properties
  public static final PropertyKey MASTER_JOURNAL_UFS_OPTION =
      new Builder(Template.MASTER_JOURNAL_UFS_OPTION).build();

  //
  // Mount table related properties
  //
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_ALLUXIO =
      new Builder(Template.MASTER_MOUNT_TABLE_ALLUXIO, "root")
          .setDefaultValue("/")
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_OPTION =
      new Builder(Template.MASTER_MOUNT_TABLE_OPTION, "root").build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_READONLY =
      new Builder(Template.MASTER_MOUNT_TABLE_READONLY, "root")
          .setDefaultValue(false)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_SHARED =
      new Builder(Template.MASTER_MOUNT_TABLE_SHARED, "root")
          .setDefaultValue(true)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_UFS =
      new Builder(Template.MASTER_MOUNT_TABLE_UFS, "root")
          .setDefaultValue(String.format("${%s}", Name.UNDERFS_ADDRESS))
          .build();

  /**
   * Master related properties.
   *
   * @deprecated since version 1.3 and will be removed in version 2.0, use MASTER_HOSTNAME instead.
   */
  @Deprecated
  public static final PropertyKey MASTER_ADDRESS = new Builder(Name.MASTER_ADDRESS).build();
  public static final PropertyKey MASTER_BIND_HOST =
      new Builder(Name.MASTER_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey MASTER_CONNECTION_TIMEOUT_MS =
      new Builder(Name.MASTER_CONNECTION_TIMEOUT_MS)
          .setDefaultValue("0ms")
          .build();
  public static final PropertyKey MASTER_FILE_ASYNC_PERSIST_HANDLER =
      new Builder(Name.MASTER_FILE_ASYNC_PERSIST_HANDLER)
          .setDefaultValue("alluxio.master.file.async.DefaultAsyncPersistHandler")
          .build();
  public static final PropertyKey MASTER_FORMAT_FILE_PREFIX =
      new Builder(Name.MASTER_FORMAT_FILE_PREFIX)
          .setDefaultValue("_format_")
          .build();
  public static final PropertyKey MASTER_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.MASTER_HEARTBEAT_INTERVAL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey MASTER_HOSTNAME = new Builder(Name.MASTER_HOSTNAME).build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS)
          .setDefaultValue("5ms")
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_TIMEOUT_MS)
          .setDefaultValue("5min")
          .build();
  public static final PropertyKey MASTER_JOURNAL_FOLDER =
      new Builder(Name.MASTER_JOURNAL_FOLDER)
          .setDefaultValue(String.format("${%s}/journal", Name.WORK_DIR))
          .build();
  /**
   * @deprecated since 1.5.0 and will be removed in 2.0.
   */
  @Deprecated
  public static final PropertyKey MASTER_JOURNAL_FORMATTER_CLASS =
      new Builder(Name.MASTER_JOURNAL_FORMATTER_CLASS)
          .setDefaultValue("alluxio.master.journalv0.ProtoBufJournalFormatter")
          .build();
  public static final PropertyKey MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
      new Builder(Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX)
          .setDefaultValue("10MB")
          .build();
  public static final PropertyKey MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS)
          .setDefaultValue("5sec")
          .build();
  public static final PropertyKey MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES =
      new Builder(Name.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES)
          .setDefaultValue(2000000)
          .build();
  public static final PropertyKey MASTER_JOURNAL_GC_PERIOD_MS =
      new Builder(Name.MASTER_JOURNAL_GC_PERIOD_MS)
          .setDefaultValue("2min")
          .build();
  public static final PropertyKey MASTER_JOURNAL_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_GC_THRESHOLD_MS)
          .setDefaultValue("5min")
          .build();
  public static final PropertyKey MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS)
          .setDefaultValue("30min")
          .build();
  public static final PropertyKey MASTER_KEYTAB_KEY_FILE =
      new Builder(Name.MASTER_KEYTAB_KEY_FILE).build();
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_CLASS =
      new Builder(Name.MASTER_LINEAGE_CHECKPOINT_CLASS)
          .setDefaultValue("alluxio.master.lineage.checkpoint.CheckpointLatestPlanner")
          .build();
  public static final PropertyKey MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS =
      new Builder(Name.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS)
          .setDefaultValue("5min")
          .build();
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS =
      new Builder(Name.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS)
          .setDefaultValue("5min")
          .build();
  public static final PropertyKey MASTER_LINEAGE_RECOMPUTE_LOG_PATH =
      new Builder(Name.MASTER_LINEAGE_RECOMPUTE_LOG_PATH)
          .setDefaultValue(String.format("${%s}/recompute.log", Name.LOGS_DIR))
          .build();
  public static final PropertyKey MASTER_PRINCIPAL = new Builder(Name.MASTER_PRINCIPAL).build();
  /**
   * @deprecated since version 1.4 and will be removed in version 2.0,
   * use USER_RPC_RETRY_MAX_NUM_RETRY instead.
   */
  @Deprecated
  public static final PropertyKey MASTER_RETRY =
      new Builder(Name.MASTER_RETRY)
          .setDefaultValue(String.format("${%s}", Name.USER_RPC_RETRY_MAX_NUM_RETRY))
          .build();
  public static final PropertyKey MASTER_RPC_PORT =
      new Builder(Name.MASTER_RPC_PORT)
          .setDefaultValue(19998)
          .build();
  public static final PropertyKey MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
      new Builder(Name.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS)
          .setDefaultValue("MEM")
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS)
          .setDefaultValue("SSD")
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS)
          .setDefaultValue("HDD")
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVELS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVELS)
          .setDefaultValue(3)
          .build();
  public static final PropertyKey MASTER_TTL_CHECKER_INTERVAL_MS =
      new Builder(Name.MASTER_TTL_CHECKER_INTERVAL_MS)
          .setDefaultValue("1hour")
          .build();
  public static final PropertyKey MASTER_UFS_PATH_CACHE_CAPACITY =
      new Builder(Name.MASTER_UFS_PATH_CACHE_CAPACITY)
          .setDefaultValue(100000)
          .build();
  public static final PropertyKey MASTER_UFS_PATH_CACHE_THREADS =
      new Builder(Name.MASTER_UFS_PATH_CACHE_THREADS)
          .setDefaultValue(64)
          .build();
  public static final PropertyKey MASTER_WEB_BIND_HOST =
      new Builder(Name.MASTER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey MASTER_WEB_HOSTNAME =
      new Builder(Name.MASTER_WEB_HOSTNAME).build();
  public static final PropertyKey MASTER_WEB_PORT =
      new Builder(Name.MASTER_WEB_PORT)
          .setDefaultValue(19999)
          .build();
  public static final PropertyKey MASTER_WHITELIST =
      new Builder(Name.MASTER_WHITELIST)
          .setDefaultValue("/")
          .build();
  public static final PropertyKey MASTER_WORKER_THREADS_MAX =
      new Builder(Name.MASTER_WORKER_THREADS_MAX)
          .setDefaultValue(2048)
          .build();
  public static final PropertyKey MASTER_WORKER_THREADS_MIN =
      new Builder(Name.MASTER_WORKER_THREADS_MIN)
          .setDefaultValue(512)
          .build();
  public static final PropertyKey MASTER_WORKER_TIMEOUT_MS =
      new Builder(Name.MASTER_WORKER_TIMEOUT_MS)
          .setDefaultValue("5min")
          .build();

  //
  // Worker related properties
  //
  public static final PropertyKey WORKER_ALLOCATOR_CLASS =
      new Builder(Name.WORKER_ALLOCATOR_CLASS)
          .setDefaultValue("alluxio.worker.block.allocator.MaxFreeAllocator")
          .build();
  public static final PropertyKey WORKER_BIND_HOST =
      new Builder(Name.WORKER_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS)
          .setDefaultValue("5min")
          .build();
  public static final PropertyKey WORKER_BLOCK_THREADS_MAX =
      new Builder(Name.WORKER_BLOCK_THREADS_MAX)
          .setDefaultValue(2048)
          .build();
  public static final PropertyKey WORKER_BLOCK_THREADS_MIN =
      new Builder(Name.WORKER_BLOCK_THREADS_MIN)
          .setDefaultValue(256)
          .build();
  public static final PropertyKey WORKER_DATA_BIND_HOST =
      new Builder(Name.WORKER_DATA_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey WORKER_DATA_FOLDER =
      new Builder(Name.WORKER_DATA_FOLDER)
          .setDefaultValue("/alluxioworker/")
          .build();
  public static final PropertyKey WORKER_DATA_HOSTNAME =
      new Builder(Name.WORKER_DATA_HOSTNAME).build();
  public static final PropertyKey WORKER_DATA_PORT =
      new Builder(Name.WORKER_DATA_PORT)
          .setDefaultValue(29999)
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_CLASS =
      new Builder(Name.WORKER_DATA_SERVER_CLASS)
          .setDefaultValue("alluxio.worker.netty.NettyDataServer")
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS =
      new Builder(Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS)
          .setDefaultValue("")
          .build();
  public static final PropertyKey WORKER_DATA_TMP_FOLDER =
      new Builder(Name.WORKER_DATA_TMP_FOLDER)
          .setDefaultValue(".tmp_blocks")
          .build();
  public static final PropertyKey WORKER_DATA_TMP_SUBDIR_MAX =
      new Builder(Name.WORKER_DATA_TMP_SUBDIR_MAX)
          .setDefaultValue(1024)
          .build();
  public static final PropertyKey WORKER_EVICTOR_CLASS =
      new Builder(Name.WORKER_EVICTOR_CLASS)
          .setDefaultValue("alluxio.worker.block.evictor.LRUEvictor")
          .build();
  public static final PropertyKey WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
      new Builder(Name.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR)
          .setDefaultValue(2.0)
          .build();
  public static final PropertyKey WORKER_EVICTOR_LRFU_STEP_FACTOR =
      new Builder(Name.WORKER_EVICTOR_LRFU_STEP_FACTOR)
          .setDefaultValue(0.25)
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_POOL_SIZE =
      new Builder(Name.WORKER_FILE_PERSIST_POOL_SIZE)
          .setDefaultValue(64)
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT =
      new Builder(Name.WORKER_FILE_PERSIST_RATE_LIMIT)
          .setDefaultValue("2GB")
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
      new Builder(Name.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey WORKER_FILE_BUFFER_SIZE =
      new Builder(Name.WORKER_FILE_BUFFER_SIZE)
          .setDefaultValue("1MB")
          .build();
  public static final PropertyKey WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey WORKER_HOSTNAME = new Builder(Name.WORKER_HOSTNAME).build();
  public static final PropertyKey WORKER_KEYTAB_FILE = new Builder(Name.WORKER_KEYTAB_FILE).build();
  public static final PropertyKey WORKER_MEMORY_SIZE =
      new Builder(Name.WORKER_MEMORY_SIZE)
          .setDefaultValue("1GB")
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BACKLOG =
      new Builder(Name.WORKER_NETWORK_NETTY_BACKLOG).build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BOSS_THREADS =
      new Builder(Name.WORKER_NETWORK_NETTY_BOSS_THREADS)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
      new Builder(Name.WORKER_NETWORK_NETTY_BUFFER_RECEIVE).build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_SEND =
      new Builder(Name.WORKER_NETWORK_NETTY_BUFFER_SEND).build();
  public static final PropertyKey WORKER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.WORKER_NETWORK_NETTY_CHANNEL).build();
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
      new Builder(Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE)
          .setDefaultValue("MAPPED")
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
      new Builder(Name.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD)
          .setDefaultValue(2)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
      new Builder(Name.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT)
          .setDefaultValue(15)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_HIGH =
      new Builder(Name.WORKER_NETWORK_NETTY_WATERMARK_HIGH)
          .setDefaultValue("32KB")
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_LOW =
      new Builder(Name.WORKER_NETWORK_NETTY_WATERMARK_LOW)
          .setDefaultValue("8KB")
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WORKER_THREADS =
      new Builder(Name.WORKER_NETWORK_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      new Builder(Name.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      new Builder(Name.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX)
          .setDefaultValue(2048)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX)
          .setDefaultValue(1024)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX)
          .setDefaultValue(128)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX)
          .setDefaultValue(1024)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_RPC_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_RPC_THREADS_MAX)
          .setDefaultValue(2048)
          .build();
  // The default is set to 11. One client is reserved for some light weight operations such as
  // heartbeat. The other 10 clients are used by commitBlock issued from the worker to the block
  // master.
  public static final PropertyKey WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE =
      new Builder(Name.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE)
          .setDefaultValue(11)
          .build();

  public static final PropertyKey WORKER_PRINCIPAL = new Builder(Name.WORKER_PRINCIPAL).build();
  public static final PropertyKey WORKER_RPC_PORT =
      new Builder(Name.WORKER_RPC_PORT)
          .setDefaultValue(29998)
          .build();
  public static final PropertyKey WORKER_SESSION_TIMEOUT_MS =
      new Builder(Name.WORKER_SESSION_TIMEOUT_MS)
          .setDefaultValue("1min")
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
      new Builder(Name.WORKER_TIERED_STORE_BLOCK_LOCK_READERS)
          .setDefaultValue(1000)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCKS =
      new Builder(Name.WORKER_TIERED_STORE_BLOCK_LOCKS)
          .setDefaultValue(1000)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 0)
          .setDefaultValue("MEM")
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 0)
          .setDefaultValue("/mnt/ramdisk")
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 0)
          .setDefaultValue("${alluxio.worker.memory.size}")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   * Use {@link #WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO} and
   * {@link #WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO} instead.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 0).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 0)
          .setDefaultValue(1.0)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 0)
          .setDefaultValue(0.7)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 1).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 1).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 1).build();
  /**
   * @deprecated It will be removed in 2.0.0.
   * Use {@link #WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO} and
   * {@link #WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO} instead.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 1).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 1)
          .setDefaultValue(1.0)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 1)
          .setDefaultValue(0.7)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 2).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 2).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 2).build();
  /**
   * @deprecated It will be removed in 2.0.0.
   * Use {@link #WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO} and
   * {@link #WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO} instead.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 2).build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 2)
          .setDefaultValue(1.0)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 2)
          .setDefaultValue(0.7)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVELS =
      new Builder(Name.WORKER_TIERED_STORE_LEVELS)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_ENABLED =
      new Builder(Name.WORKER_TIERED_STORE_RESERVER_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
      new Builder(Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RETRY =
      new Builder(Name.WORKER_TIERED_STORE_RETRY)
          .setDefaultValue(3)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_FREE_SPACE_RATIO =
      new Builder(Name.WORKER_TIERED_STORE_FREE_SPACE_RATIO)
          .setDefaultValue(0.0f)
          .build();
  public static final PropertyKey WORKER_WEB_BIND_HOST =
      new Builder(Name.WORKER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey WORKER_WEB_HOSTNAME =
      new Builder(Name.WORKER_WEB_HOSTNAME).build();
  public static final PropertyKey WORKER_WEB_PORT =
      new Builder(Name.WORKER_WEB_PORT)
          .setDefaultValue(30000)
          .build();
  public static final PropertyKey WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS =
      new Builder(Name.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS)
          .setDefaultValue("5min")
          .build();

  //
  // Proxy related properties
  //
  public static final PropertyKey PROXY_STREAM_CACHE_TIMEOUT_MS =
      new Builder(Name.PROXY_STREAM_CACHE_TIMEOUT_MS)
          .setDefaultValue("1hour")
          .build();
  public static final PropertyKey PROXY_WEB_BIND_HOST =
      new Builder(Name.PROXY_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .build();
  public static final PropertyKey PROXY_WEB_HOSTNAME =
      new Builder(Name.PROXY_WEB_HOSTNAME).build();
  public static final PropertyKey PROXY_WEB_PORT =
      new Builder(Name.PROXY_WEB_PORT)
          .setDefaultValue(39999)
          .build();

  //
  // User related properties
  //
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_THREADS =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_THREADS)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES)
          .setDefaultValue("8MB")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_BLOCK_REMOTE_READER_CLASS =
      new Builder(Name.USER_BLOCK_REMOTE_READER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyRemoteBlockReader")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_BLOCK_REMOTE_WRITER_CLASS =
      new Builder(Name.USER_BLOCK_REMOTE_WRITER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyRemoteBlockWriter")
          .build();
  public static final PropertyKey USER_BLOCK_SIZE_BYTES_DEFAULT =
      new Builder(Name.USER_BLOCK_SIZE_BYTES_DEFAULT)
          .setDefaultValue("512MB")
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_THREADS =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_THREADS)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX)
          .setDefaultValue(128)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setDefaultValue(300 * Constants.SECOND_MS)
          .build();
  public static final PropertyKey USER_DATE_FORMAT_PATTERN =
      new Builder(Name.USER_DATE_FORMAT_PATTERN)
          .setDefaultValue("MM-dd-yyyy HH:mm:ss:SSS")
          .build();
  public static final PropertyKey USER_FAILED_SPACE_REQUEST_LIMITS =
      new Builder(Name.USER_FAILED_SPACE_REQUEST_LIMITS)
          .setDefaultValue(3)
          .build();
  public static final PropertyKey USER_FILE_BUFFER_BYTES =
      new Builder(Name.USER_FILE_BUFFER_BYTES)
          .setDefaultValue("8MB")
          .build();
  public static final PropertyKey USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
      new Builder(Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey USER_FILE_COPY_FROM_LOCAL_WRITE_LOCATION_POLICY =
      new Builder(Name.USER_FILE_COPY_FROM_LOCAL_WRITE_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.RoundRobinPolicy")
          .build();
  public static final PropertyKey USER_FILE_DELETE_UNCHECKED =
      new Builder(Name.USER_FILE_DELETE_UNCHECKED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey USER_FILE_MASTER_CLIENT_THREADS =
      new Builder(Name.USER_FILE_MASTER_CLIENT_THREADS)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey USER_FILE_METADATA_LOAD_TYPE =
      new Builder(Name.USER_FILE_METADATA_LOAD_TYPE)
          .setDefaultValue("Once")
          .build();
  public static final PropertyKey USER_FILE_PASSIVE_CACHE_ENABLED =
      new Builder(Name.USER_FILE_PASSIVE_CACHE_ENABLED)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey USER_FILE_READ_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_READ_TYPE_DEFAULT)
          .setDefaultValue("CACHE_PROMOTE")
          .build();
  public static final PropertyKey USER_FILE_SEEK_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES)
          .setDefaultValue("1MB")
          .build();
  public static final PropertyKey USER_FILE_WAITCOMPLETED_POLL_MS =
      new Builder(Name.USER_FILE_WAITCOMPLETED_POLL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey USER_FILE_WORKER_CLIENT_THREADS =
      new Builder(Name.USER_FILE_WORKER_CLIENT_THREADS)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX =
      new Builder(Name.USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX)
          .setDefaultValue(128)
          .build();
  public static final PropertyKey USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setDefaultValue(300 * Constants.SECOND_MS)
          .build();
  public static final PropertyKey USER_FILE_WRITE_LOCATION_POLICY =
      new Builder(Name.USER_FILE_WRITE_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.LocalFirstPolicy")
          .build();
  public static final PropertyKey USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
      new Builder(Name.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES)
          .setDefaultValue("0MB")
          .build();
  public static final PropertyKey USER_FILE_WRITE_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_WRITE_TYPE_DEFAULT)
          .setDefaultValue("MUST_CACHE")
          .build();
  public static final PropertyKey USER_FILE_WRITE_TIER_DEFAULT =
      new Builder(Name.USER_FILE_WRITE_TIER_DEFAULT)
          .setDefaultValue(Constants.FIRST_TIER)
          .build();
  public static final PropertyKey USER_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.USER_HEARTBEAT_INTERVAL_MS)
          .setDefaultValue("1sec")
          .build();
  public static final PropertyKey USER_HOSTNAME = new Builder(Name.USER_HOSTNAME).build();
  public static final PropertyKey USER_LINEAGE_ENABLED =
      new Builder(Name.USER_LINEAGE_ENABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey USER_LINEAGE_MASTER_CLIENT_THREADS =
      new Builder(Name.USER_LINEAGE_MASTER_CLIENT_THREADS)
          .setDefaultValue(10)
          .build();
  public static final PropertyKey USER_LOCAL_READER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_READER_PACKET_SIZE_BYTES)
          .setDefaultValue("8MB")
          .build();
  public static final PropertyKey USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_WRITER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL).build();
  public static final PropertyKey USER_NETWORK_NETTY_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_NETTY_TIMEOUT_MS)
          .setDefaultValue(30000)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WORKER_THREADS =
      new Builder(Name.USER_NETWORK_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX)
          .setDefaultValue(1024)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS)
          .setDefaultValue(300 * Constants.SECOND_MS)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS)
          .setDefaultValue(300000)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      new Builder(Name.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_READER_CANCEL_ENABLED =
      new Builder(Name.USER_NETWORK_NETTY_READER_CANCEL_ENABLED)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .build();
  public static final PropertyKey USER_RPC_RETRY_BASE_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_BASE_SLEEP_MS)
          .setDefaultValue(50)
          .build();
  public static final PropertyKey USER_RPC_RETRY_MAX_NUM_RETRY =
      new Builder(Name.USER_RPC_RETRY_MAX_NUM_RETRY)
          .setDefaultValue(20)
          .build();
  public static final PropertyKey USER_RPC_RETRY_MAX_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_MAX_SLEEP_MS)
          .setDefaultValue("30sec")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_ENABLED =
      new Builder(Name.USER_UFS_DELEGATION_ENABLED)
          .setDefaultValue(true)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES)
          .setDefaultValue("8MB")
          .build();
  public static final PropertyKey USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES)
          .setDefaultValue("2MB")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_FILE_READER_CLASS =
      new Builder(Name.USER_UFS_FILE_READER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyUnderFileSystemFileReader")
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_FILE_WRITER_CLASS =
      new Builder(Name.USER_UFS_FILE_WRITER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyUnderFileSystemFileWriter")
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY =
      new Builder(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.LocalFirstPolicy")
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
      new Builder(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
      new Builder(Name.USER_UFS_BLOCK_READ_CONCURRENCY_MAX)
          .setDefaultValue(Integer.MAX_VALUE)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_OPEN_TIMEOUT_MS =
      new Builder(Name.USER_UFS_BLOCK_OPEN_TIMEOUT_MS)
          .setDefaultValue(300000)
          .build();
  public static final PropertyKey USER_SHORT_CIRCUIT_ENABLED =
      new Builder(Name.USER_SHORT_CIRCUIT_ENABLED)
          .setDefaultValue(true)
          .build();

  //
  // FUSE integration related properties
  //
  /** Maximum number of Alluxio paths to cache for fuse conversion. */
  public static final PropertyKey FUSE_CACHED_PATHS_MAX =
      new Builder(Name.FUSE_CACHED_PATHS_MAX)
          .setDefaultValue(500)
          .build();
  /** Have the fuse process log every FS request. */
  public static final PropertyKey FUSE_DEBUG_ENABLED =
      new Builder(Name.FUSE_DEBUG_ENABLED)
          .setDefaultValue(false)
          .build();

  /** FUSE file system name. */
  public static final PropertyKey FUSE_FS_NAME =
      new Builder(Name.FUSE_FS_NAME)
          .setDefaultValue("alluxio-fuse")
          .build();
  public static final PropertyKey FUSE_FS_ROOT =
      new Builder(Name.FUSE_FS_ROOT)
          .setDefaultValue("/")
          .build();
  /**
   * Passed to fuse-mount, maximum granularity of write operations:
   * Capped by the kernel to 128KB max (as of Linux 3.16.0),.
   */
  public static final PropertyKey FUSE_MAXWRITE_BYTES =
      new Builder(Name.FUSE_MAXWRITE_BYTES)
          .setDefaultValue(131072)
          .build();
  public static final PropertyKey FUSE_MOUNT_DEFAULT =
      new Builder(Name.FUSE_MOUNT_DEFAULT)
          .setDefaultValue("/mnt/alluxio")
          .build();

  //
  // Security related properties
  //
  public static final PropertyKey SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
      new Builder(Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS).build();
  public static final PropertyKey SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS =
      new Builder(Name.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS)
          .setDefaultValue("10min")
          .build();
  public static final PropertyKey SECURITY_AUTHENTICATION_TYPE =
      new Builder(Name.SECURITY_AUTHENTICATION_TYPE)
          .setDefaultValue("SIMPLE")
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED)
          .setDefaultValue(true)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP)
          .setDefaultValue("supergroup")
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_UMASK =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_UMASK)
          .setDefaultValue("022")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
      new Builder(Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS)
          .setDefaultValue("1min")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_CLASS =
      new Builder(Name.SECURITY_GROUP_MAPPING_CLASS)
          .setDefaultValue("alluxio.security.group.provider.ShellBasedUnixGroupsMapping")
          .build();
  public static final PropertyKey SECURITY_LOGIN_USERNAME =
      new Builder(Name.SECURITY_LOGIN_USERNAME).build();

  //
  // Mesos and Yarn related properties
  //
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_CPU =
      new Builder(Name.INTEGRATION_MASTER_RESOURCE_CPU)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_MEM =
      new Builder(Name.INTEGRATION_MASTER_RESOURCE_MEM)
          .setDefaultValue("1024MB")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_JAR_URL =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_JAR_URL)
          .setDefaultValue(String.format(
              "http://downloads.alluxio.org/downloads/files/${%s}/alluxio-${%s}-bin.tar.gz",
              Name.VERSION, Name.VERSION))
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NAME =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME)
          .setDefaultValue("AlluxioMaster")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_WORKER_NAME =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME)
          .setDefaultValue("AlluxioWorker")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_JDK_PATH =
      new Builder(Name.INTEGRATION_MESOS_JDK_PATH)
          .setDefaultValue("jdk1.7.0_79")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_JDK_URL =
      new Builder(Name.INTEGRATION_MESOS_JDK_URL)
          .setDefaultValue("https://alluxio-mesos.s3.amazonaws.com/jdk-7u79-linux-x64.tar.gz")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_PRINCIPAL =
      new Builder(Name.INTEGRATION_MESOS_PRINCIPAL)
          .setDefaultValue("alluxio")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ROLE =
      new Builder(Name.INTEGRATION_MESOS_ROLE)
          .setDefaultValue("*")
          .build();
  public static final PropertyKey INTEGRATION_MESOS_SECRET =
      new Builder(Name.INTEGRATION_MESOS_SECRET).build();
  public static final PropertyKey INTEGRATION_MESOS_USER =
      new Builder(Name.INTEGRATION_MESOS_USER)
          .setDefaultValue("")
          .build();
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_CPU =
      new Builder(Name.INTEGRATION_WORKER_RESOURCE_CPU)
          .setDefaultValue(1)
          .build();
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_MEM =
      new Builder(Name.INTEGRATION_WORKER_RESOURCE_MEM)
          .setDefaultValue("1024MB")
          .build();
  public static final PropertyKey INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
      new Builder(Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX)
          .setDefaultValue(1)
          .build();

  /**
   * A nested class to hold named string constants for their corresponding properties.
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
    public static final String NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS =
        "alluxio.network.netty.heartbeat.timeout.ms";
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
    public static final String UNDERFS_OBJECT_STORE_SERVICE_THREADS =
        "alluxio.underfs.object.store.service.threads";
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
    public static final String UNDERFS_S3A_DIRECTORY_SUFFIX =
        "alluxio.underfs.s3a.directory.suffix";
    public static final String UNDERFS_S3A_LIST_OBJECTS_VERSION_1 =
        "alluxio.underfs.s3a.list.objects.v1";
    public static final String UNDERFS_S3A_REQUEST_TIMEOUT_MS =
        "alluxio.underfs.s3a.request.timeout.ms";
    public static final String UNDERFS_S3A_SECURE_HTTP_ENABLED =
        "alluxio.underfs.s3a.secure.http.enabled";
    public static final String UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
        "alluxio.underfs.s3a.server.side.encryption.enabled";
    public static final String UNDERFS_S3A_SIGNER_ALGORITHM =
        "alluxio.underfs.s3a.signer.algorithm";
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
    public static final String MASTER_UFS_PATH_CACHE_CAPACITY =
        "alluxio.master.ufs.path.cache.capacity";
    public static final String MASTER_UFS_PATH_CACHE_THREADS =
        "alluxio.master.ufs.path.cache.threads";
    public static final String MASTER_WEB_BIND_HOST = "alluxio.master.web.bind.host";
    public static final String MASTER_WEB_HOSTNAME = "alluxio.master.web.hostname";
    public static final String MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String MASTER_WHITELIST = "alluxio.master.whitelist";
    public static final String MASTER_WORKER_THREADS_MAX = "alluxio.master.worker.threads.max";
    public static final String MASTER_WORKER_THREADS_MIN = "alluxio.master.worker.threads.min";
    public static final String MASTER_WORKER_TIMEOUT_MS = "alluxio.master.worker.timeout.ms";
    public static final String MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES =
        "alluxio.master.journal.checkpoint.period.entries";
    public static final String MASTER_JOURNAL_GC_PERIOD_MS = "alluxio.master.journal.gc.period.ms";
    public static final String MASTER_JOURNAL_GC_THRESHOLD_MS =
        "alluxio.master.journal.gc.threshold.ms";
    public static final String MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
        "alluxio.master.journal.temporary.file.gc.threshold.ms";

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
    public static final String WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS =
        "alluxio.worker.data.server.domain.socket.address";
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
    public static final String WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
        "alluxio.worker.network.netty.block.reader.threads.max";
    public static final String WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
        "alluxio.worker.network.netty.block.writer.threads.max";
    public static final String WORKER_NETWORK_NETTY_FILE_READER_THREADS_MAX =
        "alluxio.worker.network.netty.file.reader.threads.max";
    public static final String WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
        "alluxio.worker.network.netty.file.writer.threads.max";
    public static final String WORKER_NETWORK_NETTY_RPC_THREADS_MAX =
        "alluxio.worker.network.netty.rpc.threads.max";
    public static final String WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE =
        "alluxio.worker.block.master.client.pool.size";
    public static final String WORKER_PRINCIPAL = "alluxio.worker.principal";
    public static final String WORKER_RPC_PORT = "alluxio.worker.port";
    public static final String WORKER_SESSION_TIMEOUT_MS = "alluxio.worker.session.timeout.ms";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
        "alluxio.worker.tieredstore.block.lock.readers";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCKS =
        "alluxio.worker.tieredstore.block.locks";
    public static final String WORKER_TIERED_STORE_LEVELS = "alluxio.worker.tieredstore.levels";
    public static final String WORKER_TIERED_STORE_RESERVER_ENABLED =
        "alluxio.worker.tieredstore.reserver.enabled";
    public static final String WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
        "alluxio.worker.tieredstore.reserver.interval.ms";
    public static final String WORKER_TIERED_STORE_RETRY = "alluxio.worker.tieredstore.retry";
    public static final String WORKER_TIERED_STORE_FREE_SPACE_RATIO
        = "alluxio.worker.tieredstore.free.space.ratio";
    public static final String WORKER_WEB_BIND_HOST = "alluxio.worker.web.bind.host";
    public static final String WORKER_WEB_HOSTNAME = "alluxio.worker.web.hostname";
    public static final String WORKER_WEB_PORT = "alluxio.worker.web.port";
    public static final String WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS =
        "alluxio.worker.ufs.block.open.timeout.ms";

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
    public static final String USER_FILE_COPY_FROM_LOCAL_WRITE_LOCATION_POLICY =
            "alluxio.user.file.copyfromlocal.write.location.policy.class";
    public static final String USER_FILE_DELETE_UNCHECKED =
        "alluxio.user.file.delete.unchecked";
    public static final String USER_FILE_MASTER_CLIENT_THREADS =
        "alluxio.user.file.master.client.threads";
    public static final String USER_FILE_METADATA_LOAD_TYPE =
        "alluxio.user.file.metadata.load.type";
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
    public static final String USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS =
        "alluxio.user.network.netty.writer.close.timeout.ms";
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
    public static final String USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
        "alluxio.user.network.netty.reader.packet.size.bytes";
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
    public static final String USER_SHORT_CIRCUIT_ENABLED = "alluxio.user.short.circuit.enabled";

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

  /**
   * A set of templates to generate the names of parameterized properties given
   * different parameters. E.g., * {@code Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
   */
  @ThreadSafe
  public enum Template {
    MASTER_JOURNAL_UFS_OPTION("alluxio.master.journal.ufs.option",
        "alluxio\\.master\\.journal\\.ufs\\.option"),
    MASTER_JOURNAL_UFS_OPTION_PROPERTY("alluxio.master.journal.ufs.option.%s",
        "alluxio\\.master\\.journal\\.ufs\\.option(\\.\\w+)++"),
    MASTER_MOUNT_TABLE_ALLUXIO("alluxio.master.mount.table.%s.alluxio",
        "alluxio\\.master\\.mount\\.table.(\\w+)\\.alluxio"),
    MASTER_MOUNT_TABLE_OPTION("alluxio.master.mount.table.%s.option",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option"),
    MASTER_MOUNT_TABLE_OPTION_PROPERTY("alluxio.master.mount.table.%s.option.%s",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option(\\.\\w+)++"),
    MASTER_MOUNT_TABLE_READONLY("alluxio.master.mount.table.%s.readonly",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.readonly"),
    MASTER_MOUNT_TABLE_SHARED("alluxio.master.mount.table.%s.shared",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.shared"),
    MASTER_MOUNT_TABLE_UFS("alluxio.master.mount.table.%s.ufs",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.ufs"),
    MASTER_MOUNT_TABLE_ROOT_OPTION_PROPERTY("alluxio.master.mount.table.root.option.%s",
        "alluxio\\.master\\.mount\\.table\\.root\\.option(\\.\\w+)++"),
    MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS("alluxio.master.tieredstore.global.level%d.alias",
        "alluxio\\.master\\.tieredstore\\.global\\.level(\\d+)\\.alias"),
    WORKER_TIERED_STORE_LEVEL_ALIAS("alluxio.worker.tieredstore.level%d.alias",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.alias"),
    WORKER_TIERED_STORE_LEVEL_DIRS_PATH("alluxio.worker.tieredstore.level%d.dirs.path",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.path"),
    WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA("alluxio.worker.tieredstore.level%d.dirs.quota",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.quota"),
    WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO("alluxio.worker.tieredstore.level%d.reserved.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.reserved\\.ratio"),
    WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO(
        "alluxio.worker.tieredstore.level%d.watermark.high.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.watermark\\.high\\.ratio"),
    WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO(
        "alluxio.worker.tieredstore.level%d.watermark.low.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.watermark\\.low\\.ratio"),
    ;

    private final String mFormat;
    private final Pattern mPattern;

    /**
     * Constructs a property key format.
     *
     * @param format String of this property as formatted string
     * @param re String of this property as regexp
     */
    Template(String format, String re) {
      mFormat = format;
      mPattern = Pattern.compile(re);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("format", mFormat).add("pattern", mPattern)
          .toString();
    }

    /**
     * Converts a property key template (e.g.,
     * {@link #WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO}) to a {@link PropertyKey} instance.
     *
     * @param params ordinal
     * @return corresponding property
     */
    public PropertyKey format(Object... params) {
      return new PropertyKey(String.format(mFormat, params));
    }
  }

  /**
   * @param input string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String input) {
    // Check if input matches any default keys
    if (DEFAULT_KEYS_MAP.containsKey(input)) {
      return true;
    }
    // Check if input matches any parameterized keys
    for (Template template : Template.values()) {
      Matcher matcher = template.mPattern.matcher(input);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param input string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String input) {
    // First try to parse it as default key
    PropertyKey key = DEFAULT_KEYS_MAP.get(input);
    if (key != null) {
      return key;
    }
    // Try different templates and see if any template matches
    for (Template template : Template.values()) {
      Matcher matcher = template.mPattern.matcher(input);
      if (matcher.matches()) {
        return new PropertyKey(input);
      }
    }
    throw new IllegalArgumentException(
        ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(input));
  }

  /**
   * @return all pre-defined property keys
   */
  public static Collection<? extends PropertyKey> defaultKeys() {
    return DEFAULT_KEYS_MAP.values();
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
   * Factory method to create a constant default property
   * and assign a default value together with its alias.
   *
   * @param name String of this property
   * @param defaultValue Default value of this property in compile time if not null
   * @param aliases String list of aliases of this property
   */
  static PropertyKey create(String name, Object defaultValue, String[] aliases) {
    PropertyKey key = new PropertyKey(name);
    DEFAULT_KEYS_MAP.put(name, key);
    DEFAULT_VALUES.put(key, defaultValue);
    if (aliases != null) {
      for (String alias : aliases) {
        DEFAULT_KEYS_MAP.put(alias, key);
      }
    }
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

  /**
   * @param name the name of a property key
   * @return if this property key is deprecated
   */
  public static boolean isDeprecated(String name) {
    try {
      PropertyKey key = new PropertyKey(name);
      Class c = key.getClass();
      Field field = c.getDeclaredField(name);
      Annotation[] annotations = field.getDeclaredAnnotations();
      for (Annotation anno : annotations) {
        if (anno instanceof Deprecated) {
          return true;
        }
      }
      return false;
    } catch (NoSuchFieldException e) {
      return false;
    }
  }
}
