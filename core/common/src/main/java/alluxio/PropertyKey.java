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
import alluxio.util.OSUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.Scope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.annotation.Annotation;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration property keys. This class provides a set of pre-defined property keys.
 */
@ThreadSafe
public final class PropertyKey implements Comparable<PropertyKey> {
  // The following two maps must be the first to initialize within this file.
  /** A map from default property key's string name to the key. */
  private static final Map<String, PropertyKey> DEFAULT_KEYS_MAP = new ConcurrentHashMap<>();
  /** A map from default property key's alias to the key. */
  private static final Map<String, PropertyKey> DEFAULT_ALIAS_MAP = new ConcurrentHashMap<>();

  /**
   * The consistency check level to apply to a certain property key.
   * User can run "alluxio validateEnv all cluster.conf.consistent" to validate the consistency of
   * configuration properties within the cluster. The command reads all Alluxio properties on
   * each node of the cluster and shows error or warning for properties that have inconsistent
   * values among the nodes. For example, it would show error if the Alluxio property for aws secret
   * access key has some value on a master node and a different value on one of the worker nodes.
   */
  public enum ConsistencyCheckLevel {
    /**
     * Do not check consistency of property value.
     * This should be set if the property is not required to have same value among the nodes
     * (e.g. worker hostname).
     */
    IGNORE,
    /**
     * Check consistency of property value, show warning of inconsistent values.
     * This should be set if the property is recommended to have same value among the nodes,
     * although having different values does not cause immediate issues(e.g. alluxio home folder
     * location, timeout value for connecting to a worker).
     */
    WARN,
    /**
     * Check consistency of property value, show error of inconsistent values.
     * This should be set if the property is required to have same value among the nodes
     * (e.g. AWS credentials, journal location).
     */
    ENFORCE,
  }

  /**
   * Indicates how the property value should be displayed.
   */
  public enum DisplayType {
    /**
     * The property value should be displayed normally.
     */
    DEFAULT,
    /**
     * The property value contains credentials and should not be displayed directly.
     */
    CREDENTIALS,
  }

  /**
   * Builder to create {@link PropertyKey} instances. Note that, <code>Builder.build()</code> will
   * throw exception if there is an existing property built with the same name.
   */
  public static final class Builder {
    private String[] mAlias;
    private DefaultSupplier mDefaultSupplier;
    private Object mDefaultValue;
    private String mDescription;
    private String mName;
    private boolean mIgnoredSiteProperty;
    private boolean mIsBuiltIn = true;
    private boolean mIsHidden;
    private ConsistencyCheckLevel mConsistencyCheckLevel = ConsistencyCheckLevel.IGNORE;
    private Scope mScope = Scope.ALL;
    private DisplayType mDisplayType = DisplayType.DEFAULT;

    /**
     * @param name name of the property
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param template template for the property name
     * @param params parameters of the template
     */
    public Builder(PropertyKey.Template template, Object... params) {
      mName = String.format(template.mFormat, params);
    }

    /**
     * @param alias aliases for the property
     * @return the updated builder instance
     */
    public Builder setAlias(String[] alias) {
      mAlias = Arrays.copyOf(alias, alias.length);
      return this;
    }

    /**
     * @param name name for the property
     * @return the updated builder instance
     */
    public Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param defaultSupplier supplier for the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultSupplier(DefaultSupplier defaultSupplier) {
      mDefaultSupplier = defaultSupplier;
      return this;
    }

    /**
     * @param supplier supplier for the property's default value
     * @param description description of the default value
     * @return the updated builder instance
     */
    public Builder setDefaultSupplier(Supplier<Object> supplier, String description) {
      mDefaultSupplier = new DefaultSupplier(supplier, description);
      return this;
    }

    /**
     * @param defaultValue the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultValue(Object defaultValue) {
      mDefaultValue = defaultValue;
      return this;
    }

    /**
     * @param description of the property
     * @return the updated builder instance
     */
    public Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * @param isBuiltIn whether to the property is a built-in Alluxio property
     * @return the updated builder instance
     */
    public Builder setIsBuiltIn(boolean isBuiltIn) {
      mIsBuiltIn = isBuiltIn;
      return this;
    }

    /**
     * @param isHidden whether to hide the property when generating property documentation
     * @return the updated builder instance
     */
    public Builder setIsHidden(boolean isHidden) {
      mIsHidden = isHidden;
      return this;
    }

    /**
     * @param ignoredSiteProperty whether the property should be ignored in alluxio-site.properties
     * @return the updated builder instance
     */
    public Builder setIgnoredSiteProperty(boolean ignoredSiteProperty) {
      mIgnoredSiteProperty = ignoredSiteProperty;
      return this;
    }

    /**
     * @param consistencyCheckLevel the consistency level that applies to this property
     * @return the updated builder instance
     */
    public Builder setConsistencyCheckLevel(ConsistencyCheckLevel consistencyCheckLevel) {
      mConsistencyCheckLevel = consistencyCheckLevel;
      return this;
    }

    /**
     * @param scope which components this property applies to
     * @return the updated builder instance
     */
    public Builder setScope(Scope scope) {
      mScope = scope;
      return this;
    }

    /**
     * @param displayType the displayType that indicates how the property value should be displayed
     * @return the updated builder instance
     */
    public Builder setDisplayType(DisplayType displayType) {
      mDisplayType = displayType;
      return this;
    }

    /**
     * Creates and registers the property key.
     *
     * @return the created property key instance
     */
    public PropertyKey build() {
      PropertyKey key = buildUnregistered();
      Preconditions.checkState(PropertyKey.register(key), "Cannot register existing key \"%s\"",
          mName);
      return key;
    }

    /**
     * Creates the property key without registering it with default property list.
     *
     * @return the created property key instance
     */
    public PropertyKey buildUnregistered() {
      DefaultSupplier defaultSupplier = mDefaultSupplier;
      if (defaultSupplier == null) {
        String defaultString = String.valueOf(mDefaultValue);
        defaultSupplier = (mDefaultValue == null)
            ? new DefaultSupplier(() -> null, "null")
            : new DefaultSupplier(() -> defaultString, defaultString);
      }

      PropertyKey key = new PropertyKey(mName, mDescription, defaultSupplier, mAlias,
          mIgnoredSiteProperty, mIsHidden, mConsistencyCheckLevel, mScope, mDisplayType,
          mIsBuiltIn);
      return key;
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
          .setDescription("The directory containing files used to configure Alluxio.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey CONF_VALIDATION_ENABLED =
      new Builder(Name.CONF_VALIDATION_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to validate the configuration properties when initializing "
              + "Alluxio clients or server process.")
          .setIsHidden(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey DEBUG =
      new Builder(Name.DEBUG)
          .setDefaultValue(false)
          .setDescription("Set to true to enable debug mode which has additional logging and "
              + "info in the Web UI.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey EXTENSIONS_DIR =
      new Builder(Name.EXTENSIONS_DIR)
          .setDefaultValue(String.format("${%s}/extensions", Name.HOME))
          .setDescription("The directory containing Alluxio extensions.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey HOME =
      new Builder(Name.HOME)
          .setDefaultValue("/opt/alluxio")
          .setDescription("Alluxio installation directory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey KEY_VALUE_ENABLED =
      new Builder(Name.KEY_VALUE_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether the key-value service is enabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey KEY_VALUE_PARTITION_SIZE_BYTES_MAX =
      new Builder(Name.KEY_VALUE_PARTITION_SIZE_BYTES_MAX)
          .setDefaultValue("512MB")
          .setDescription(String.format(
              "Maximum allowable size of a single key-value partition "
                  + "in a store. This value should be no larger than the block size "
                  + "(%s).", Name.USER_BLOCK_SIZE_BYTES_DEFAULT))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey LOGGER_TYPE =
      new Builder(Name.LOGGER_TYPE)
          .setDefaultValue("Console")
          .setDescription("The type of logger.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .build();
  public static final PropertyKey LOGS_DIR =
      new Builder(Name.LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", Name.WORK_DIR))
          .setDescription("The path to store log files.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey METRICS_CONF_FILE =
      new Builder(Name.METRICS_CONF_FILE)
          .setDefaultValue(String.format("${%s}/metrics.properties", Name.CONF_DIR))
          .setDescription("The file path of the metrics system configuration file. By default "
              + "it is `metrics.properties` in the `conf` directory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey METRICS_CONTEXT_SHUTDOWN_TIMEOUT =
      new Builder(Name.METRICS_CONTEXT_SHUTDOWN_TIMEOUT)
          .setDefaultValue("1sec")
          .setDescription("Time to wait for the metrics context to shut down. The main purpose for "
              + "this property is to allow tests to shut down faster.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setIsHidden(true)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      new Builder(Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.network.host.resolution.timeout.ms"})
          .setDefaultValue("5sec")
          .setDescription("During startup of the Master and Worker processes Alluxio needs to "
              + "ensure that they are listening on externally resolvable and reachable host "
              + "names. To do this, Alluxio will automatically attempt to select an "
              + "appropriate host name if one was not explicitly specified. This represents "
              + "the maximum amount of time spent waiting to determine if a candidate host "
              + "name is resolvable over the network.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS =
      new Builder(Name.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS)
          .setAlias(new String[] {"alluxio.network.netty.heartbeat.timeout.ms"})
          .setDefaultValue("30sec")
          .setDescription("The amount of time the server will wait before closing a netty "
              + "connection if there has not been any incoming traffic. The client will "
              + "periodically heartbeat when there is no activity on a connection. This value "
              + "should be the same on the clients and server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  /**
   * @deprecated since 1.8.0 and will be removed in 2.0.
   */
  @Deprecated
  public static final PropertyKey NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
      new Builder(Name.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX)
          .setDefaultValue("16MB")
          .setDescription("(Experimental) The largest allowable frame size used for Thrift "
              + "RPC communication.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey SITE_CONF_DIR =
      new Builder(Name.SITE_CONF_DIR)
          .setDefaultSupplier(
              () -> String.format("${%s}/,%s/.alluxio/,/etc/alluxio/",
                  Name.CONF_DIR, System.getProperty("user.home")),
              String.format("${%s}/,${user.home}/.alluxio/,/etc/alluxio/", Name.CONF_DIR))
          .setDescription(
              String.format("Comma-separated search path for %s.", Constants.SITE_PROPERTIES))
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey TEST_MODE =
      new Builder(Name.TEST_MODE)
          .setDefaultValue(false)
          .setDescription("Flag used only during tests to allow special behavior.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey TMP_DIRS =
      new Builder(Name.TMP_DIRS)
          .setDefaultValue("/tmp")
          .setDescription("The path(s) to store Alluxio temporary files, use commas as delimiters. "
              + "If multiple paths are specified, one will be selected at random per temporary "
              + "file. Currently, only files to be uploaded to object stores are stored in these "
              + "paths.")
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey VERSION =
      new Builder(Name.VERSION)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setDefaultValue(ProjectConstants.VERSION)
          .setDescription("Version of Alluxio. User should never modify this property.")
          .setIgnoredSiteProperty(true)
          .setIsHidden(true)
          .build();
  public static final PropertyKey WEB_FILE_INFO_ENABLED =
      new Builder(Name.WEB_FILE_INFO_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether detailed file information are enabled for the web UI.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_RESOURCES =
      new Builder(Name.WEB_RESOURCES)
          .setDefaultValue(String.format("${%s}/core/server/common/src/main/webapp", Name.HOME))
          .setDescription("Path to the web application resources.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_TEMP_PATH =
      new Builder(Name.WEB_TEMP_PATH)
          .setDefaultValue(String.format("${%s}/web/", Name.WORK_DIR))
          .setDescription("Path to store temporary web server files.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_THREADS =
      new Builder(Name.WEB_THREADS)
          .setDefaultValue(1)
          .setDescription("How many threads to use for the web server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WORK_DIR =
      new Builder(Name.WORK_DIR)
          .setDefaultValue(String.format("${%s}", Name.HOME))
          .setDescription("The directory to use for Alluxio's working directory. By default, "
              + "the journal, logs, and under file system data (if using local filesystem) "
              + "are written here.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey ZOOKEEPER_ADDRESS =
      new Builder(Name.ZOOKEEPER_ADDRESS)
          .setDescription("Address of ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey ZOOKEEPER_CONNECTION_TIMEOUT =
      new Builder(Name.ZOOKEEPER_CONNECTION_TIMEOUT)
          .setDefaultValue("15s") // matches Zookeeper's default
          .setDescription("Connection timeout to use when connecting to Zookeeper")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey ZOOKEEPER_ELECTION_PATH =
      new Builder(Name.ZOOKEEPER_ELECTION_PATH)
          .setDefaultValue("/election")
          .setDescription("Election directory in ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey ZOOKEEPER_ENABLED =
      new Builder(Name.ZOOKEEPER_ENABLED)
          .setDefaultValue(false)
          .setDescription("If true, setup master fault tolerant mode using ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      new Builder(Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT)
          .setDefaultValue(10)
          .setDescription("The number of retries to inquire leader from ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_PATH =
      new Builder(Name.ZOOKEEPER_LEADER_PATH)
          .setDefaultValue("/leader")
          .setDescription("Leader directory in ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey ZOOKEEPER_SESSION_TIMEOUT =
      new Builder(Name.ZOOKEEPER_SESSION_TIMEOUT)
          .setDefaultValue("60s") // matches Zookeeper's default
          .setDescription("Session timeout to use when connecting to Zookeeper")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
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
          .setDescription("Alluxio directory in the under file system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_ALLOW_SET_OWNER_FAILURE =
      new Builder(Name.UNDERFS_ALLOW_SET_OWNER_FAILURE)
          .setDefaultValue(false)
          .setDescription("Whether to allow setting owner in UFS to fail. When set to true, "
              + "it is possible file or directory owners diverge between Alluxio and UFS.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey UNDERFS_CLEANUP_ENABLED =
      new Builder(Name.UNDERFS_CLEANUP_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether or not to clean up under filesystem periodically."
              + "Some ufs operations may not be completed and cleaned up successfully "
              + "in normal ways and leave some intermediate data that needs periodical cleanup."
              + "If enabled, all the mount points will be cleaned up when a leader master starts "
              + "or cleanup interval is reached. This should be used sparingly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey UNDERFS_CLEANUP_INTERVAL =
      new Builder(Name.UNDERFS_CLEANUP_INTERVAL)
          .setDefaultValue("1day")
          .setDescription("The interval for periodically cleaning all the "
              + " mounted under filesystem.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey UNDERFS_LISTING_LENGTH =
      new Builder(Name.UNDERFS_LISTING_LENGTH)
          .setDefaultValue(1000)
          .setDescription("The maximum number of directory entries to list in a single query "
              + "to under file system. If the total number of entries is greater than the "
              + "specified length, multiple queries will be issued.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
      new Builder(Name.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING)
          .setDescription("Optionally, specify a preset gcs owner id to Alluxio username "
              + "static mapping in the format \"id1=user1;id2=user2\". The Google Cloud "
              + "Storage IDs can be found at the console address "
              + "https://console.cloud.google.com/storage/settings . Please use the "
              + "\"Owners\" one.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_HDFS_CONFIGURATION =
      new Builder(Name.UNDERFS_HDFS_CONFIGURATION)
          .setDefaultValue(String.format(
              "${%s}/core-site.xml:${%s}/hdfs-site.xml", Name.CONF_DIR, Name.CONF_DIR))
          .setDescription("Location of the HDFS configuration file.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_HDFS_IMPL =
      new Builder(Name.UNDERFS_HDFS_IMPL)
          .setDefaultValue("org.apache.hadoop.hdfs.DistributedFileSystem")
          .setDescription("The implementation class of the HDFS as the under storage system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_HDFS_PREFIXES =
      new Builder(Name.UNDERFS_HDFS_PREFIXES)
          .setDefaultValue("hdfs://,glusterfs:///,maprfs:///")
          .setDescription("Optionally, specify which prefixes should run through the HDFS "
              + "implementation of UnderFileSystem. The delimiter is any whitespace "
              + "and/or ','.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_HDFS_REMOTE =
      new Builder(Name.UNDERFS_HDFS_REMOTE)
          .setDefaultValue(false)
          .setDescription("Boolean indicating whether or not the under storage worker nodes "
              + "are remote with respect to Alluxio worker nodes. If set to true, Alluxio "
              + "will not attempt to discover locality information from the under storage "
              + "because locality is impossible. This will improve performance. The default "
              + "value is false.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_SERVICE_THREADS =
      new Builder(Name.UNDERFS_OBJECT_STORE_SERVICE_THREADS)
          .setDefaultValue(20)
          .setDescription("The number of threads in executor pool for parallel object store "
              + "UFS operations.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
      new Builder(Name.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY)
          .setDefaultValue(false)
          .setDescription("Whether or not to share object storage under storage system "
              + "mounted point with all Alluxio users. Note that this configuration has no "
              + "effect on HDFS nor local UFS.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_READ_RETRY_BASE_SLEEP_MS =
      new Builder(Name.UNDERFS_OBJECT_STORE_READ_RETRY_BASE_SLEEP_MS)
          .setDefaultValue("50ms")
          .setDescription("Block reads from an object store automatically retry for transient "
              + "errors with an exponential backoff. This property determines the base time in the "
              + "exponential backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_READ_RETRY_MAX_NUM =
      new Builder(Name.UNDERFS_OBJECT_STORE_READ_RETRY_MAX_NUM)
          .setDefaultValue(20)
          .setDescription("Block reads from  an object store automatically retry for transient "
              + "errors with an exponential backoff. This property determines the maximum number of"
              + " retries.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_READ_RETRY_MAX_SLEEP_MS =
      new Builder(Name.UNDERFS_OBJECT_STORE_READ_RETRY_MAX_SLEEP_MS)
          .setDefaultValue("30sec")
          .setDescription("Block reads from an object store automatically retry for transient "
              + "errors with an exponential backoff. This property determines the maximum wait time"
              + " in the backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_MAX =
      new Builder(Name.UNDERFS_OSS_CONNECT_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of OSS connections.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_TIMEOUT =
      new Builder(Name.UNDERFS_OSS_CONNECT_TIMEOUT)
          .setAlias(new String[]{"alluxio.underfs.oss.connection.timeout.ms"})
          .setDefaultValue("50sec")
          .setDescription("The timeout when connecting to OSS.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OSS_CONNECT_TTL =
      new Builder(Name.UNDERFS_OSS_CONNECT_TTL)
          .setDefaultValue(-1)
          .setDescription("The TTL of OSS connections in ms.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OSS_SOCKET_TIMEOUT =
      new Builder(Name.UNDERFS_OSS_SOCKET_TIMEOUT)
          .setAlias(new String[]{"alluxio.underfs.oss.socket.timeout.ms"})
          .setDefaultValue("50sec")
          .setDescription("The timeout of OSS socket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_ADMIN_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_ADMIN_THREADS_MAX)
          .setDefaultValue(20)
          .setDescription("The maximum number of threads to use for metadata operations when "
              + "communicating with S3. These operations may be fairly concurrent and "
              + "frequent but should not take much time to process.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_DISABLE_DNS_BUCKETS =
      new Builder(Name.UNDERFS_S3_DISABLE_DNS_BUCKETS)
          .setDefaultValue(false)
          .setDescription("Optionally, specify to make all S3 requests path style.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_ENDPOINT =
      new Builder(Name.UNDERFS_S3_ENDPOINT)
          .setDescription("Optionally, to reduce data latency or visit resources which are "
              + "separated in different AWS regions, specify a regional endpoint to make aws "
              + "requests. An endpoint is a URL that is the entry point for a web service. "
              + "For example, s3.cn-north-1.amazonaws.com.cn is an entry point for the Amazon "
              + "S3 service in beijing region.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
      new Builder(Name.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING)
          .setDescription("Optionally, specify a preset s3 canonical id to Alluxio username "
              + "static mapping, in the format \"id1=user1;id2=user2\". The AWS S3 canonical "
              + "ID can be found at the console address "
              + "https://console.aws.amazon.com/iam/home?#security_credential . Please expand "
              + "the \"Account Identifiers\" tab and refer to \"Canonical User ID\".")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_HOST =
      new Builder(Name.UNDERFS_S3_PROXY_HOST)
          .setDescription("Optionally, specify a proxy host for communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_PORT =
      new Builder(Name.UNDERFS_S3_PROXY_PORT)
          .setDescription("Optionally, specify a proxy port for communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_THREADS_MAX)
          .setDefaultValue(40)
          .setDescription("The maximum number of threads to use for communicating with S3 and "
              + "the maximum number of concurrent connections to S3. Includes both threads "
              + "for data upload and metadata operations. This number should be at least as "
              + "large as the max admin threads plus max upload threads.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_UPLOAD_THREADS_MAX =
      new Builder(Name.UNDERFS_S3_UPLOAD_THREADS_MAX)
          .setDefaultValue(20)
          .setDescription("The maximum number of threads to use for uploading data to S3 for "
              + "multipart uploads. These operations can be fairly expensive, so multiple "
              + "threads are encouraged. However, this also splits the bandwidth between "
              + "threads, meaning the overall latency for completing an upload will be higher "
              + "for more threads.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
      new Builder(Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.underfs.s3a.consistency.timeout.ms"})
          .setDefaultValue("1min")
          .setDescription("The duration to wait for metadata consistency from the under "
              + "storage. This is only used by internal Alluxio operations which should be "
              + "successful, but may appear unsuccessful due to eventual consistency.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_DIRECTORY_SUFFIX =
      new Builder(Name.UNDERFS_S3A_DIRECTORY_SUFFIX)
          .setDefaultValue("/")
          .setDescription("Directories are represented in S3 as zero-byte objects named with "
              + "the specified suffix.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_BULK_DELETE_ENABLED =
      new Builder(Name.UNDERFS_S3A_BULK_DELETE_ENABLED)
          .setDefaultValue(true)
          .setIsHidden(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_INHERIT_ACL =
      new Builder(Name.UNDERFS_S3A_INHERIT_ACL)
          .setDefaultValue(true)
          .setDescription("Optionally disable this to disable inheriting bucket ACLs on "
              + "objects.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE =
      new Builder(Name.UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE)
          .setDefaultValue("3day")
          .setDescription("Streaming uploads may not have been completed/aborted correctly "
              + "and need periodical ufs cleanup. If ufs cleanup is enabled, "
              + "intermediate multipart uploads in all non-readonly S3 mount points "
              + "older than this age will be cleaned. This may impact other "
              + "ongoing upload operations, so a large clean age is encouraged.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_LIST_OBJECTS_VERSION_1 =
      new Builder(Name.UNDERFS_S3A_LIST_OBJECTS_VERSION_1)
          .setDefaultValue(false)
          .setDescription("Whether to use version 1 of GET Bucket (List Objects) API.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_REQUEST_TIMEOUT =
      new Builder(Name.UNDERFS_S3A_REQUEST_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.underfs.s3a.request.timeout.ms"})
          .setDefaultValue("1min")
          .setDescription("The timeout for a single request to S3. Infinity if set to 0. "
              + "Setting this property to a non-zero value can improve performance by "
              + "avoiding the long tail of requests to S3. For very slow connections to S3, "
              + "consider increasing this value or setting it to 0.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_SECURE_HTTP_ENABLED =
      new Builder(Name.UNDERFS_S3A_SECURE_HTTP_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether or not to use HTTPS protocol when communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
      new Builder(Name.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether or not to encrypt data stored in S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_SIGNER_ALGORITHM =
      new Builder(Name.UNDERFS_S3A_SIGNER_ALGORITHM)
          .setDescription("The signature algorithm which should be used to sign requests to "
              + "the s3 service. This is optional, and if not set, the client will "
              + "automatically determine it. For interacting with an S3 endpoint which only "
              + "supports v2 signatures, set this to \"S3SignerType\".")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_SOCKET_TIMEOUT_MS =
      new Builder(Name.UNDERFS_S3A_SOCKET_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.underfs.s3a.socket.timeout.ms"})
          .setDefaultValue("50sec")
          .setDescription("Length of the socket timeout when communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_STREAMING_UPLOAD_ENABLED =
      new Builder(Name.UNDERFS_S3A_STREAMING_UPLOAD_ENABLED)
          .setDefaultValue(false)
          .setDescription("(Experimental) If true, using streaming upload to write to S3A.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE =
      new Builder(Name.UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE)
          .setDefaultValue("64MB")
          .setDescription("Maximum allowable size of a single buffer file when using "
              + "S3A streaming upload. When the buffer file reaches the partition size, "
              + "it will be uploaded and the upcoming data will write to other buffer files."
              + "If the partition size is too small, S3A upload speed might be affected. ")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();

  //
  // UFS access control related properties
  //
  // Not prefixed with fs, the s3a property names mirror the aws-sdk property names for ease of use
  public static final PropertyKey GCS_ACCESS_KEY = new Builder(Name.GCS_ACCESS_KEY)
      .setDescription("The access key of GCS bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey GCS_SECRET_KEY = new Builder(Name.GCS_SECRET_KEY)
      .setDescription("The secret key of GCS bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey OSS_ACCESS_KEY = new Builder(Name.OSS_ACCESS_KEY)
      .setDescription("The access key of OSS bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey OSS_ENDPOINT_KEY = new Builder(Name.OSS_ENDPOINT_KEY)
      .setDescription("The endpoint key of OSS bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .build();
  public static final PropertyKey OSS_SECRET_KEY = new Builder(Name.OSS_SECRET_KEY)
      .setDescription("The secret key of OSS bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey S3A_ACCESS_KEY = new Builder(Name.S3A_ACCESS_KEY)
      .setDescription("The access key of S3 bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey S3A_SECRET_KEY = new Builder(Name.S3A_SECRET_KEY)
      .setDescription("The secret key of S3 bucket.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.SERVER)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey SWIFT_API_KEY = new Builder(Name.SWIFT_API_KEY)
      .setDescription("(deprecated) The API key used for user:tenant authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_AUTH_METHOD_KEY = new Builder(Name.SWIFT_AUTH_METHOD_KEY)
      .setDescription("Choice of authentication method: "
          + "[tempauth (default), swiftauth, keystone, keystonev3].")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_AUTH_URL_KEY = new Builder(Name.SWIFT_AUTH_URL_KEY)
      .setDescription("Authentication URL for REST server, e.g., http://server:8090/auth/v1.0.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_PASSWORD_KEY = new Builder(Name.SWIFT_PASSWORD_KEY)
      .setDescription("The password used for user:tenant authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_SIMULATION = new Builder(Name.SWIFT_SIMULATION)
      .setDescription("Whether to simulate a single node Swift backend for testing purposes: "
          + "true or false (default).")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_TENANT_KEY = new Builder(Name.SWIFT_TENANT_KEY)
      .setDescription("Swift user for authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_USE_PUBLIC_URI_KEY =
      new Builder(Name.SWIFT_USE_PUBLIC_URI_KEY)
          .setDescription("Whether the REST server is in a public domain: true (default) or false.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey SWIFT_USER_KEY = new Builder(Name.SWIFT_USER_KEY)
      .setDescription("Swift tenant for authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_REGION_KEY = new Builder(Name.SWIFT_REGION_KEY)
      .setDescription("Service region when using Keystone authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey COS_ACCESS_KEY =
      new Builder(Name.COS_ACCESS_KEY)
          .setDescription("The access key of COS bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey COS_APP_ID =
      new Builder(Name.COS_APP_ID)
          .setDescription("The app id of COS bucket.")
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey COS_CONNECTION_MAX =
      new Builder(Name.COS_CONNECTION_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of COS connections.")
          .build();
  public static final PropertyKey COS_CONNECTION_TIMEOUT =
      new Builder(Name.COS_CONNECTION_TIMEOUT)
          .setDefaultValue("50sec")
          .setDescription("The timeout of connecting to COS.")
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey COS_SOCKET_TIMEOUT =
      new Builder(Name.COS_SOCKET_TIMEOUT)
          .setDefaultValue("50sec")
          .setDescription("The timeout of COS socket.")
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey COS_REGION =
      new Builder(Name.COS_REGION)
          .setDescription("The region name of COS bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey COS_SECRET_KEY =
      new Builder(Name.COS_SECRET_KEY)
          .setDescription("The secret key of COS bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  // Journal ufs related properties
  public static final PropertyKey MASTER_JOURNAL_UFS_OPTION =
      new Builder(Template.MASTER_JOURNAL_UFS_OPTION)
          .setDescription("The configuration to use for the journal operations.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();

  //
  // Mount table related properties
  //
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_ALLUXIO =
      new Builder(Template.MASTER_MOUNT_TABLE_ALLUXIO, "root")
          .setDefaultValue("/")
          .setDescription("Alluxio root mount point.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_OPTION =
      new Builder(Template.MASTER_MOUNT_TABLE_OPTION, "root")
          .setDescription("Configuration for the UFS of Alluxio root mount point.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_READONLY =
      new Builder(Template.MASTER_MOUNT_TABLE_READONLY, "root")
          .setDefaultValue(false)
          .setDescription("Whether Alluxio root mount point is readonly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_SHARED =
      new Builder(Template.MASTER_MOUNT_TABLE_SHARED, "root")
          .setDefaultValue(true)
          .setDescription("Whether Alluxio root mount point is shared.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_MOUNT_TABLE_ROOT_UFS =
      new Builder(Template.MASTER_MOUNT_TABLE_UFS, "root")
          .setDefaultValue(String.format("${%s}", Name.UNDERFS_ADDRESS))
          .setDescription("The UFS mounted to Alluxio root mount point.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();

  /**
   * Master related properties.
   */
  public static final PropertyKey MASTER_AUDIT_LOGGING_ENABLED =
      new Builder(Name.MASTER_AUDIT_LOGGING_ENABLED)
          .setDefaultValue(false)
          .setDescription("Set to true to enable file system master audit.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_AUDIT_LOGGING_QUEUE_CAPACITY =
      new Builder(Name.MASTER_AUDIT_LOGGING_QUEUE_CAPACITY)
          .setDefaultValue(10000)
          .setDescription("Capacity of the queue used by audit logging.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_DIRECTORY =
      new Builder(Name.MASTER_BACKUP_DIRECTORY)
          .setDefaultValue("/alluxio_backups")
          .setDescription("Default directory for writing master metadata backups. This path is"
              + " relative to the root directory of the root UFS. For example, if the root ufs"
              + " directory is hdfs://cluster/alluxio/data, the default backup directory will be"
              + " hdfs://cluster/alluxio_backups")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BIND_HOST =
      new Builder(Name.MASTER_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname that Alluxio master binds to. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL =
      new Builder(Name.MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL)
          .setDefaultValue("10min")
          .setDescription("Interval for removing closed client sockets from internal tracking.")
          .setIsHidden(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_CLUSTER_METRICS_UPDATE_INTERVAL =
      new Builder(Name.MASTER_CLUSTER_METRICS_UPDATE_INTERVAL)
          .setDefaultValue("1m")
          .setDescription("The interval for periodically updating the cluster level metrics.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .setIsHidden(true)
          .build();
  public static final PropertyKey MASTER_CONNECTION_TIMEOUT_MS =
      new Builder(Name.MASTER_CONNECTION_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.master.connection.timeout.ms"})
          .setDefaultValue("0")
          .setDescription("Timeout of connections between master and client. "
              + "A value of 0 means never timeout")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FILE_ASYNC_PERSIST_HANDLER =
      new Builder(Name.MASTER_FILE_ASYNC_PERSIST_HANDLER)
          .setDefaultValue("alluxio.master.file.async.DefaultAsyncPersistHandler")
          .setDescription("The handler for processing the async persistence requests.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FORMAT_FILE_PREFIX =
      new Builder(Name.MASTER_FORMAT_FILE_PREFIX)
          .setDefaultValue("_format_")
          .setDescription("The file prefix of the file generated in the journal directory "
              + "when the journal is formatted. The master will search for a file with this "
              + "prefix when determining if the journal is formatted.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_MASTER_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_MASTER_HEARTBEAT_INTERVAL)
          .setDefaultValue("2min")
          .setDescription("The interval between Alluxio masters' heartbeats.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_WORKER_HEARTBEAT_INTERVAL)
          .setAlias(new String[]{"alluxio.master.heartbeat.interval.ms",
              "alluxio.master.heartbeat.interval"})
          .setDefaultValue("10sec")
          .setDescription("The interval between Alluxio master and worker heartbeats.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey MASTER_HEARTBEAT_TIMEOUT =
      new Builder(Name.MASTER_HEARTBEAT_TIMEOUT)
          .setDefaultValue("10min")
          .setDescription("Timeout between leader master and standby master"
              + " indicating a lost master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_HOSTNAME =
      new Builder(Name.MASTER_HOSTNAME)
          .setDescription("The hostname of Alluxio master.")
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS)
          .setAlias(new String[]{"alluxio.master.journal.flush.batch.time.ms"})
          .setDefaultValue("5ms")
          .setDescription("Time to wait for batching journal writes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.master.journal.flush.timeout.ms"})
          .setDefaultValue("5min")
          .setDescription("The amount of time to keep retrying journal "
              + "writes before giving up and shutting down the master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_RETRY_INTERVAL =
      new Builder(Name.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL)
          .setDefaultValue("1sec")
          .setDescription("The amount of time to sleep between retrying journal flushes")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FOLDER =
      new Builder(Name.MASTER_JOURNAL_FOLDER)
          .setDefaultValue(String.format("${%s}/journal", Name.WORK_DIR))
          .setDescription("The path to store master journal logs.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_INIT_FROM_BACKUP =
      new Builder(Name.MASTER_JOURNAL_INIT_FROM_BACKUP)
          .setDescription("A uri for a backup to initialize the journal from. When the"
              + " master becomes primary, if it sees that its journal is freshly formatted, it will"
              + " restore its state from the backup. When running multiple masters, this property"
              + " must be configured on all masters since it isn't known during startup which"
              + " master will become the first primary.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TYPE =
      new Builder(Name.MASTER_JOURNAL_TYPE)
          .setDefaultValue("UFS")
          .setDescription("The type of journal to use. Valid options are UFS (store journal in "
              + "UFS) and NOOP (do not use a journal).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  /**
   * @deprecated since 1.5.0 and will be removed in 2.0.
   */
  @Deprecated
  public static final PropertyKey MASTER_JOURNAL_FORMATTER_CLASS =
      new Builder(Name.MASTER_JOURNAL_FORMATTER_CLASS)
          .setDefaultValue("alluxio.master.journalv0.ProtoBufJournalFormatter")
          .setDescription("The class to serialize the journal in a specified format.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
      new Builder(Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX)
          .setDefaultValue("10MB")
          .setDescription("If a log file is bigger than this value, it will rotate to next "
              + "file.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS)
          .setAlias(new String[]{"alluxio.master.journal.tailer.shutdown.quiet.wait.time.ms"})
          .setDefaultValue("5sec")
          .setDescription("Before the standby master shuts down its tailer thread, there "
              + "should be no update to the leader master's journal in this specified time "
              + "period.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS)
          .setAlias(new String[]{"alluxio.master.journal.tailer.sleep.time.ms"})
          .setDefaultValue("1sec")
          .setDescription("Time for the standby master to sleep for when it "
              + "cannot find anything new in leader master's journal.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES =
      new Builder(Name.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES)
          .setDefaultValue(2000000)
          .setDescription("The number of journal entries to write before creating a new "
              + "journal checkpoint.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_GC_PERIOD_MS =
      new Builder(Name.MASTER_JOURNAL_GC_PERIOD_MS)
          .setAlias(new String[] {"alluxio.master.journal.gc.period.ms"})
          .setDefaultValue("2min")
          .setDescription("Frequency with which to scan for and delete stale journal checkpoints.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_GC_THRESHOLD_MS)
          .setAlias(new String[]{"alluxio.master.journal.gc.threshold.ms"})
          .setDefaultValue("5min")
          .setDescription("Minimum age for garbage collecting checkpoints.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS)
          .setAlias(new String[]{"alluxio.master.journal.temporary.file.gc.threshold.ms"})
          .setDescription("Minimum age for garbage collecting temporary checkpoint files.")
          .setDefaultValue("30min")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_KEYTAB_KEY_FILE =
      new Builder(Name.MASTER_KEYTAB_KEY_FILE)
          .setDescription("Kerberos keytab file for Alluxio master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL)
          .setDefaultValue("1h")
          .setDescription("The interval for periodically logging the configuration check report.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR =
      new Builder(Name.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR)
          .setDefaultValue(false)
          .setDescription("Whether the system should delete orphaned blocks found during the "
              + "periodic integrity check. This is an experimental feature.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL =
      new Builder(Name.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL)
          .setDefaultValue("1hr")
          .setDescription("The period for the block integrity check, disabled if <= 0.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_CHECKER_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS)
          .setDefaultValue(1000)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_INITIAL_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS)
          .setDefaultValue(Constants.SECOND_MS)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS =
      new Builder(Name.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS)
          .setDefaultValue(0)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_MAX_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_MAX_INTERVAL_MS)
          .setDefaultValue(Constants.HOUR_MS)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS =
      new Builder(Name.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS)
          .setDefaultValue(Constants.DAY_MS)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS)
          .setDefaultValue(1000)
          .build();
  public static final PropertyKey MASTER_REPLICATION_CHECK_INTERVAL_MS =
      new Builder(Name.MASTER_REPLICATION_CHECK_INTERVAL_MS)
          .setDefaultValue(60000)
          .build();
  public static final PropertyKey MASTER_PRINCIPAL = new Builder(Name.MASTER_PRINCIPAL)
      .setDescription("Kerberos principal for Alluxio master.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.MASTER)
      .build();
  public static final PropertyKey MASTER_RPC_PORT =
      new Builder(Name.MASTER_RPC_PORT)
          .setDefaultValue(19998)
          .setDescription("The port that Alluxio master node runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_SERVING_THREAD_TIMEOUT =
      new Builder(Name.MASTER_SERVING_THREAD_TIMEOUT)
          .setDefaultValue("5m")
          .setDescription("When stepping down from being the primary, the master will wait this "
              + "long for the thrift serving thread to stop before giving up and shutting down "
              + "the server")
          .setIsHidden(true)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED =
      new Builder(Name.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether the system should be checked on startup for orphaned blocks "
              + "(blocks having no corresponding files but still taking system resource due to "
              + "various system failures). Orphaned blocks will be deleted during master startup "
              + "if this property is true. This property is available since 1.7.1")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
      new Builder(Name.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether the system should be checked for consistency with the "
              + "underlying storage on startup. During the time the check is running, Alluxio "
              + "will be in read only mode. Enabled by default.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_THRIFT_SHUTDOWN_TIMEOUT =
         new Builder(Name.MASTER_THRIFT_SHUTDOWN_TIMEOUT)
         .setDefaultValue("60sec")
         .setDescription("Maximum time to wait for thrift servers to stop on shutdown")
         .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
         .setScope(Scope.MASTER)
         .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS)
          .setDefaultValue("MEM")
          .setDescription("The name of the highest storage tier in the entire system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS)
          .setDefaultValue("SSD")
          .setDescription("The name of the second highest storage tier in the entire system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS)
          .setDefaultValue("HDD")
          .setDescription("The name of the third highest storage tier in the entire system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_LEVELS =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_LEVELS)
          .setDefaultValue(3)
          .setDescription("The total number of storage tiers in the system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_TTL_CHECKER_INTERVAL_MS =
      new Builder(Name.MASTER_TTL_CHECKER_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.master.ttl.checker.interval.ms"})
          .setDefaultValue("1hour")
          .setDescription("Time interval to periodically delete the files "
              + "with expired ttl value.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_BLOCK_LOCATION_CACHE_CAPACITY =
      new Builder(Name.MASTER_UFS_BLOCK_LOCATION_CACHE_CAPACITY)
          .setDefaultValue(1000000)
          .setDescription("The capacity of the UFS block locations cache. "
              + "This cache caches UFS block locations for files that are persisted "
              + "but not in Alluxio space, so that listing status of these files do not need to "
              + "repeatedly ask UFS for their block locations. If this is set to 0, the cache "
              + "will be disabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_PATH_CACHE_CAPACITY =
      new Builder(Name.MASTER_UFS_PATH_CACHE_CAPACITY)
          .setDefaultValue(100000)
          .setDescription("The capacity of the UFS path cache. This cache is used to "
              + "approximate the `Once` metadata load behavior (see "
              + "`alluxio.user.file.metadata.load.type`). Larger caches will consume more "
              + "memory, but will better approximate the `Once` behavior.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_PATH_CACHE_THREADS =
      new Builder(Name.MASTER_UFS_PATH_CACHE_THREADS)
          .setDefaultValue(64)
          .setDescription("The maximum size of the thread pool for asynchronously processing "
              + "paths for the UFS path cache. Greater number of threads will decrease the "
              + "amount of staleness in the async cache, but may impact performance. If this "
              + "is set to 0, the cache will be disabled, and "
              + "`alluxio.user.file.metadata.load.type=Once` will behave like `Always`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WEB_BIND_HOST =
      new Builder(Name.MASTER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname Alluxio master web UI binds to. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WEB_HOSTNAME =
      new Builder(Name.MASTER_WEB_HOSTNAME)
          .setDescription("The hostname of Alluxio Master web UI.")
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_WEB_PORT =
      new Builder(Name.MASTER_WEB_PORT)
          .setDefaultValue(19999)
          .setDescription("The port Alluxio web UI runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WHITELIST =
      new Builder(Name.MASTER_WHITELIST)
          .setDefaultValue("/")
          .setDescription("A comma-separated list of prefixes of the paths which are "
              + "cacheable, separated by semi-colons. Alluxio will try to cache the cacheable "
              + "file when it is read for the first time.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_CONNECT_WAIT_TIME =
      new Builder(Name.MASTER_WORKER_CONNECT_WAIT_TIME)
          .setDefaultValue("5sec")
          .setDescription("Alluxio master will wait a period of time after start up for "
              + "all workers to register, before it starts accepting client requests. "
              + "This property determines the wait time.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_THREADS_MAX =
      new Builder(Name.MASTER_WORKER_THREADS_MAX)
          .setDefaultSupplier(() -> {
            try {
              java.lang.management.OperatingSystemMXBean os =
                  ManagementFactory.getOperatingSystemMXBean();
              if (os instanceof UnixOperatingSystemMXBean) {
                return Math.min(32768, Math.max(2048,
                    ((UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount() / 3));
              }
            } catch (Exception e) {
              // Set lower limit
            }
            return 2048;
          }, "A third of the max file descriptors limit, if b/w 2048 and 32768")
          .setDescription("The maximum number of incoming RPC requests to master that can be "
              + "handled. This value is used to configure maximum number of threads in Thrift "
              + "thread pool with master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_THREADS_MIN =
      new Builder(Name.MASTER_WORKER_THREADS_MIN)
          .setDefaultValue(512)
          .setDescription("The minimum number of threads used to handle incoming RPC requests "
              + "to master. This value is used to configure minimum number of threads in "
              + "Thrift thread pool with master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_TIMEOUT_MS =
      new Builder(Name.MASTER_WORKER_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.master.worker.timeout.ms"})
          .setDefaultValue("5min")
          .setDescription("Timeout between master and worker indicating a lost worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();

  //
  // Worker related properties
  //
  public static final PropertyKey WORKER_ALLOCATOR_CLASS =
      new Builder(Name.WORKER_ALLOCATOR_CLASS)
          .setDefaultValue("alluxio.worker.block.allocator.MaxFreeAllocator")
          .setDescription("The strategy that a worker uses to allocate space among storage "
              + "directories in certain storage layer. Valid options include: "
              + "`alluxio.worker.block.allocator.MaxFreeAllocator`, "
              + "`alluxio.worker.block.allocator.GreedyAllocator`, "
              + "`alluxio.worker.block.allocator.RoundRobinAllocator`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BIND_HOST =
      new Builder(Name.WORKER_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname Alluxio's worker node binds to. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.worker.block.heartbeat.interval.ms"})
          .setDefaultValue("1sec")
          .setDescription("The interval between block workers' heartbeats.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.worker.block.heartbeat.timeout.ms"})
          .setDefaultValue(String.format("${%s}", Name.WORKER_MASTER_CONNECT_RETRY_TIMEOUT))
          .setDescription("The timeout value of block workers' heartbeats. If the worker can't "
              + "connect to master before this interval expires, the worker will exit.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_THREADS_MAX =
      new Builder(Name.WORKER_BLOCK_THREADS_MAX)
          .setDefaultValue(2048)
          .setDescription("The maximum number of incoming RPC requests to block worker that "
              + "can be handled. This value is used to configure maximum number of threads in "
              + "Thrift thread pool with block worker. This value should be greater than the "
              + "sum of `alluxio.user.block.worker.client.threads` across concurrent Alluxio "
              + "clients. Otherwise, the worker connection pool can be drained, preventing "
              + "new connections from being established.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_THREADS_MIN =
      new Builder(Name.WORKER_BLOCK_THREADS_MIN)
          .setDefaultValue(256)
          .setDescription("The minimum number of threads used to handle incoming RPC requests "
              + "to block worker. This value is used to configure minimum number of threads "
              + "in Thrift thread pool with block worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_BIND_HOST =
      new Builder(Name.WORKER_DATA_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname that the Alluxio worker's data server runs on. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_FOLDER =
      new Builder(Name.WORKER_DATA_FOLDER)
          .setDefaultValue("/alluxioworker/")
          .setDescription("A relative path within each storage directory used as the data "
              + "folder for Alluxio worker to put data for tiered store.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_FOLDER_PERMISSIONS =
      new Builder(Name.WORKER_DATA_FOLDER_PERMISSIONS)
          .setDefaultValue("rwxrwxrwx")
          .setDescription("The permission set for the worker data folder. If short circuit is used "
              + "this folder should be accessible by all users (rwxrwxrwx).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_HOSTNAME =
      new Builder(Name.WORKER_DATA_HOSTNAME)
          .setDescription("The hostname of Alluxio worker data service.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_PORT =
      new Builder(Name.WORKER_DATA_PORT)
          .setDefaultValue(29999)
          .setDescription("The port Alluxio's worker's data server runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_CLASS =
      new Builder(Name.WORKER_DATA_SERVER_CLASS)
          .setDefaultValue("alluxio.worker.netty.NettyDataServer")
          .setDescription("Selects the networking stack to run the worker with. Valid options "
              + "are: `alluxio.worker.netty.NettyDataServer`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS =
      new Builder(Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS)
          .setDescription("The path to the domain socket. Short-circuit reads make use of a "
              + "UNIX domain socket when this is set (non-empty). This is a special path in "
              + "the file system that allows the client and the AlluxioWorker to communicate. "
              + "You will need to set a path to this socket. The AlluxioWorker needs to be "
              + "able to create the path. If " + Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID
              + " is set, the path should be the home directory for the domain socket. The full "
              + "path for the domain socket with be <path>/<uuid>.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID =
      new Builder(Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)
          .setDefaultValue("false")
          .setDescription("If true, the property " + Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS
              + "is the path to the home directory for the domain socket and a unique identifier "
              + "is used as the domain socket name. In addition, clients ignore "
              + Name.USER_HOSTNAME + " while detecting a local worker for short circuit ops. "
              + "If false, the property is the absolute path to the UNIX domain socket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey WORKER_DATA_TMP_FOLDER =
      new Builder(Name.WORKER_DATA_TMP_FOLDER)
          .setDefaultValue(".tmp_blocks")
          .setDescription("A relative path in alluxio.worker.data.folder used to store the "
              + "temporary data for uncommitted files.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_TMP_SUBDIR_MAX =
      new Builder(Name.WORKER_DATA_TMP_SUBDIR_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of sub-directories allowed to be created in "
              + "alluxio.worker.data.tmp.folder.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_EVICTOR_CLASS =
      new Builder(Name.WORKER_EVICTOR_CLASS)
          .setDefaultValue("alluxio.worker.block.evictor.LRUEvictor")
          .setDescription("The strategy that a worker uses to evict block files when a "
              + "storage layer runs out of space. Valid options include "
              + "`alluxio.worker.block.evictor.LRFUEvictor`, "
              + "`alluxio.worker.block.evictor.GreedyEvictor`, "
              + "`alluxio.worker.block.evictor.LRUEvictor`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
      new Builder(Name.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR)
          .setDefaultValue(2.0)
          .setDescription("A attenuation factor in [2, INF) to control the behavior of LRFU.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_EVICTOR_LRFU_STEP_FACTOR =
      new Builder(Name.WORKER_EVICTOR_LRFU_STEP_FACTOR)
          .setDefaultValue(0.25)
          .setDescription("A factor in [0, 1] to control the behavior of LRFU: smaller value "
              + "makes LRFU more similar to LFU; and larger value makes LRFU closer to LRU.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_POOL_SIZE =
      new Builder(Name.WORKER_FILE_PERSIST_POOL_SIZE)
          .setDefaultValue(64)
          .setDescription("The size of the thread pool per worker, in which the thread "
              + "persists an ASYNC_THROUGH file to under storage.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT =
      new Builder(Name.WORKER_FILE_PERSIST_RATE_LIMIT)
          .setDefaultValue("2GB")
          .setDescription("The rate limit of asynchronous persistence per second.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
      new Builder(Name.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to enable rate limiting when performing asynchronous "
              + "persistence.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FILE_BUFFER_SIZE =
      new Builder(Name.WORKER_FILE_BUFFER_SIZE)
          .setDefaultValue("1MB")
          .setDescription("The buffer size for worker to write data into the tiered storage.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.worker.filesystem.heartbeat.interval.ms"})
          .setDefaultValue("1sec")
          .setDescription("The heartbeat interval between the worker and file system master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_FREE_SPACE_TIMEOUT =
      new Builder(Name.WORKER_FREE_SPACE_TIMEOUT)
          .setDefaultValue("10sec")
          .setDescription("The duration for which a worker will wait for eviction to make space "
              + "available for a client write request.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_HOSTNAME = new Builder(Name.WORKER_HOSTNAME)
      .setDescription("The hostname of Alluxio worker.")
      .setScope(Scope.WORKER)
      .build();
  public static final PropertyKey WORKER_KEYTAB_FILE = new Builder(Name.WORKER_KEYTAB_FILE)
      .setDescription("Kerberos keytab file for Alluxio worker.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.WORKER)
      .build();
  public static final PropertyKey WORKER_MEMORY_SIZE =
      new Builder(Name.WORKER_MEMORY_SIZE)
          .setDefaultSupplier(() -> {
            try {
              OperatingSystemMXBean operatingSystemMXBean =
                  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
              return operatingSystemMXBean.getTotalPhysicalMemorySize() * 2 / 3;
            } catch (Exception e) {
              // The package com.sun.management may not be available on every platform.
              // fallback to a reasonable size.
              return "1GB";
            }
          }, "2/3 of total system memory, or 1GB if system memory size cannot be determined")
          .setDescription("Memory capacity of each worker node.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BACKLOG =
      new Builder(Name.WORKER_NETWORK_NETTY_BACKLOG)
          .setDescription("Netty socket option for SO_BACKLOG: the number of connections queued.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BOSS_THREADS =
      new Builder(Name.WORKER_NETWORK_NETTY_BOSS_THREADS)
          .setDefaultValue(1)
          .setDescription("How many threads to use for accepting new requests.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_RECEIVE =
      new Builder(Name.WORKER_NETWORK_NETTY_BUFFER_RECEIVE)
          .setDescription("Netty socket option for SO_RCVBUF: the proposed buffer size that will "
              + "be used for receives.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BUFFER_SEND =
      new Builder(Name.WORKER_NETWORK_NETTY_BUFFER_SEND)
          .setDescription("Netty socket option for SO_SNDBUF: the proposed buffer size that will "
              + "be used for sends.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MASTER_CONNECT_RETRY_TIMEOUT =
      new Builder(Name.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)
          .setDescription("Retry period before workers give up on connecting to master")
          .setDefaultValue("1hour")
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.WORKER_NETWORK_NETTY_CHANNEL)
          .setDescription("Netty channel type: NIO or EPOLL. If EPOLL is not available, this will "
              + "automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .setDefaultValue("EPOLL")
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE =
      new Builder(Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE)
          .setDefaultValue("MAPPED")
          .setDescription("When returning files to the user, select how the data is "
              + "transferred; valid options are `MAPPED` (uses java MappedByteBuffer) and "
              + "`TRANSFER` (uses Java FileChannel.transferTo).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
      new Builder(Name.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD)
          .setDefaultValue("2sec")
          .setDescription("The quiet period. When the netty server is shutting "
              + "down, it will ensure that no RPCs occur during the quiet period. If an RPC "
              + "occurs, then the quiet period will restart before shutting down the netty "
              + "server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT =
      new Builder(Name.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT)
          .setDefaultValue("15sec")
          .setDescription("Maximum amount of time to wait until the netty server "
              + "is shutdown (regardless of the quiet period).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_HIGH =
      new Builder(Name.WORKER_NETWORK_NETTY_WATERMARK_HIGH)
          .setDefaultValue("32KB")
          .setDescription("Determines how many bytes can be in the write queue before "
              + "switching to non-writable.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WATERMARK_LOW =
      new Builder(Name.WORKER_NETWORK_NETTY_WATERMARK_LOW)
          .setDefaultValue("8KB")
          .setDescription("Once the high watermark limit is reached, the queue must be "
              + "flushed down to the low watermark before switching back to writable.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WORKER_THREADS =
      new Builder(Name.WORKER_NETWORK_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .setDescription("How many threads to use for processing requests. Zero defaults to "
              + "#cpuCores * 2.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      new Builder(Name.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .setDescription("The maximum number of parallel data packets when a client writes to a "
              + "worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      new Builder(Name.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .setDescription("The maximum number of parallel data packets when a client reads from a "
              + "worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_ASYNC_CACHE_MANAGER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_ASYNC_CACHE_MANAGER_THREADS_MAX)
          .setDefaultValue(8)
          .setDescription("The maximum number of threads used to cache blocks asynchronously in "
              + "the netty data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX)
          .setDefaultValue(2048)
          .setDescription("The maximum number of threads used to read blocks in the netty "
              + "data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of threads used to write blocks in the netty "
              + "data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of threads used to write files to UFS in the "
              + "netty data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_NETTY_RPC_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_NETTY_RPC_THREADS_MAX)
          .setDefaultValue(2048)
          .setDescription("The maximum number of threads used to handle worker side RPCs in "
              + "the netty data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  // The default is set to 11. One client is reserved for some light weight operations such as
  // heartbeat. The other 10 clients are used by commitBlock issued from the worker to the block
  // master.
  public static final PropertyKey WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE =
      new Builder(Name.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE)
          .setDefaultValue(11)
          .setDescription("The block master client pool size on the Alluxio workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();

  public static final PropertyKey WORKER_PRINCIPAL = new Builder(Name.WORKER_PRINCIPAL)
      .setDescription("Kerberos principal for Alluxio worker.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.WORKER)
      .build();
  public static final PropertyKey WORKER_RPC_PORT =
      new Builder(Name.WORKER_RPC_PORT)
          .setDefaultValue(29998)
          .setDescription("The port Alluxio's worker node runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_SESSION_TIMEOUT_MS =
      new Builder(Name.WORKER_SESSION_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.worker.session.timeout.ms"})
          .setDefaultValue("1min")
          .setDescription("Timeout between worker and client connection "
              + "indicating a lost session connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
      new Builder(Name.WORKER_TIERED_STORE_BLOCK_LOCK_READERS)
          .setDefaultValue(1000)
          .setDescription("The max number of concurrent readers for a block lock.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_BLOCK_LOCKS =
      new Builder(Name.WORKER_TIERED_STORE_BLOCK_LOCKS)
          .setDefaultValue(1000)
          .setDescription("Total number of block locks for an Alluxio block worker. Larger "
              + "value leads to finer locking granularity, but uses more space.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 0)
          .setDefaultValue("MEM")
          .setDescription("The alias of the top storage tier on this worker. It must "
              + "match one of the global storage tiers from the master configuration. We "
              + "disable placing an alias lower in the global hierarchy before an alias with "
              + "a higher postion on the worker hierarchy. So by default, SSD cannot come "
              + "before MEM on any worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 0)
          .setDefaultSupplier(() -> OSUtils.isLinux() ? "/mnt/ramdisk" : "/Volumes/ramdisk",
              "/mnt/ramdisk on Linux, /Volumes/ramdisk on OSX")
          .setDescription("The path of storage directory for the top storage tier. Note "
              + "for MacOS the value should be `/Volumes/`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 0)
          .setDefaultValue("${alluxio.worker.memory.size}")
          .setDescription("The capacity of the top storage tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 0)
          .setDescription("Fraction of space reserved in the top storage tier. "
              + "This has been deprecated, please use high and low watermark instead.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 0)
          .setDefaultValue(0.95)
          .setDescription("The high watermark of the space in the top storage tier (a value "
              + "between 0 and 1).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 0)
          .setDefaultValue(0.7)
          .setDescription("The low watermark of the space in the top storage tier (a value "
              + "between 0 and 1).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 1)
          .setDescription("The alias of the second storage tier on this worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 1)
          .setDescription("The path of storage directory for the second storage tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 1)
          .setDescription("The capacity of the second storage tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 1)
          .setDescription("Fraction of space reserved in the second storage tier. "
              + "This has been deprecated, please use high and low watermark instead.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 1)
          .setDescription("The high watermark of the space in the second storage tier (a value "
              + "between 0 and 1).")
          .setDefaultValue(0.95)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 1)
          .setDefaultValue(0.7)
          .setDescription("The low watermark of the space in the second storage tier (a value "
              + "between 0 and 1).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 2)
          .setDescription("The alias of the third storage tier on this worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_PATH =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH, 2)
          .setDescription("The path of storage directory for the third storage tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 2)
          .setDescription("The capacity of the third storage tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO, 2)
          .setDescription("Fraction of space reserved in the third storage tier. "
              + "This has been deprecated, please use high and low watermark instead.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO, 2)
          .setDefaultValue(0.95)
          .setDescription("The high watermark of the space in the third storage tier (a value "
              + "between 0 and 1).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO, 2)
          .setDefaultValue(0.7)
          .setDescription("The low watermark of the space in the third storage tier (a value "
              + "between 0 and 1).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVELS =
      new Builder(Name.WORKER_TIERED_STORE_LEVELS)
          .setDefaultValue(1)
          .setDescription("The number of storage tiers on the worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_ENABLED =
      new Builder(Name.WORKER_TIERED_STORE_RESERVER_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to enable tiered store reserver service or not.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
      new Builder(Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.worker.tieredstore.reserver.interval.ms"})
          .setDefaultValue("1sec")
          .setDescription("The time period of space reserver service, which "
              + "keeps certain portion of available space on each layer.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_RETRY =
      new Builder(Name.WORKER_TIERED_STORE_RETRY)
          .setDefaultValue(3)
          .setDescription("The number of retries that the worker uses to process blocks.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_WEB_BIND_HOST =
      new Builder(Name.WORKER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname Alluxio worker's web server binds to. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_WEB_HOSTNAME =
      new Builder(Name.WORKER_WEB_HOSTNAME)
          .setDescription("The hostname Alluxio worker's web UI binds to.")
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_WEB_PORT =
      new Builder(Name.WORKER_WEB_PORT)
          .setDefaultValue(30000)
          .setDescription("The port Alluxio worker's web UI runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS =
      new Builder(Name.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.worker.ufs.block.open.timeout.ms"})
          .setDefaultValue("5min")
          .setDescription("Timeout to open a block from UFS.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_UFS_INSTREAM_CACHE_ENABLED =
      new Builder(Name.WORKER_UFS_INSTREAM_CACHE_ENABLED)
          .setDefaultValue("true")
          .setDescription("Enable caching for seekable under storage input stream, "
              + "so that subsequent seek operations on the same file will reuse "
              + "the cached input stream. This will improve position read performance "
              + "as the open operations of some under file system would be expensive. "
              + "The cached input stream would be stale, when the UFS file is modified "
              + "without notifying alluxio. ")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME =
      new Builder(Name.WORKER_UFS_INSTREAM_CACHE_EXPIRATION_TIME)
          .setDefaultValue("5min")
          .setDescription("Cached UFS instream expiration time.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_UFS_INSTREAM_CACHE_MAX_SIZE =
      new Builder(Name.WORKER_UFS_INSTREAM_CACHE_MAX_SIZE)
          .setDefaultValue("5000")
          .setDescription("The max entries in the UFS instream cache.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();

  //
  // Proxy related properties
  //
  public static final PropertyKey PROXY_S3_WRITE_TYPE =
      new Builder(Name.PROXY_S3_WRITE_TYPE)
          .setDefaultValue("CACHE_THROUGH")
          .setDescription("Write type when creating buckets and objects through S3 API. "
              + "Valid options are "
              + "`MUST_CACHE` (write will only go to Alluxio and must be stored in Alluxio), "
              + "`CACHE_THROUGH` (try to cache, write to UnderFS synchronously), "
              + "`THROUGH` (no cache, write to UnderFS synchronously).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_S3_DELETE_TYPE =
      new Builder(Name.PROXY_S3_DELETE_TYPE)
          .setDefaultValue(Constants.S3_DELETE_IN_ALLUXIO_AND_UFS)
          .setDescription(String.format(
              "Delete type when deleting buckets and objects through S3 API. Valid options are "
                  + "`%s` (delete both in Alluxio and UFS), "
                  + "`%s` (delete only the buckets or objects in Alluxio namespace).",
              Constants.S3_DELETE_IN_ALLUXIO_AND_UFS, Constants.S3_DELETE_IN_ALLUXIO_ONLY))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_S3_MULTIPART_TEMPORARY_DIR_SUFFIX =
      new Builder(Name.PROXY_S3_MULTIPART_TEMPORARY_DIR_SUFFIX)
          .setDefaultValue(Constants.S3_MULTIPART_TEMPORARY_DIR_SUFFIX)
          .setDescription("Suffix for the directory which holds parts during a multipart upload.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey PROXY_STREAM_CACHE_TIMEOUT_MS =
      new Builder(Name.PROXY_STREAM_CACHE_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.proxy.stream.cache.timeout.ms"})
          .setDefaultValue("1hour")
          .setDescription("The timeout for the input and output streams cache eviction in the "
              + "proxy.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_WEB_BIND_HOST =
      new Builder(Name.PROXY_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname that the Alluxio proxy's web server runs on. See <a "
              + "href=\"#configure-multihomed-networks\">multi-homed networks</a>.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_WEB_HOSTNAME =
      new Builder(Name.PROXY_WEB_HOSTNAME)
          .setDescription("The hostname Alluxio proxy's web UI binds to.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_WEB_PORT =
      new Builder(Name.PROXY_WEB_PORT)
          .setDefaultValue(39999)
          .setDescription("The port Alluxio proxy's web UI runs on.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();

  //
  // Locality related properties
  //
  public static final PropertyKey LOCALITY_ORDER =
      new Builder(Name.LOCALITY_ORDER)
          .setDefaultValue(String.format("%s,%s", Constants.LOCALITY_NODE, Constants.LOCALITY_RACK))
          .setDescription("Ordering of locality tiers")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey LOCALITY_SCRIPT =
      new Builder(Name.LOCALITY_SCRIPT)
          .setDefaultValue(Constants.ALLUXIO_LOCALITY_SCRIPT)
          .setDescription("A script to determine tiered identity for locality checking")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey LOCALITY_TIER_NODE =
      new Builder(Template.LOCALITY_TIER, Constants.LOCALITY_NODE)
          .setDescription("Value to use for determining node locality")
          .build();
  public static final PropertyKey LOCALITY_TIER_RACK =
      new Builder(Template.LOCALITY_TIER, Constants.LOCALITY_RACK)
          .setDescription("Value to use for determining rack locality")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();

  public static final PropertyKey LOCALITY_COMPARE_NODE_IP =
          new Builder(Name.LOCALITY_COMPARE_NODE_IP)
          .setDefaultValue(false)
          .setDescription("Whether try to resolve the node IP address for locality checking")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();

  //
  // Log server related properties
  //
  public static final PropertyKey LOGSERVER_LOGS_DIR =
      new Builder(Name.LOGSERVER_LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", Name.WORK_DIR))
          .setDescription("Default location for remote log files.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey LOGSERVER_HOSTNAME =
      new Builder(Name.LOGSERVER_HOSTNAME)
          .setDescription("The hostname of Alluxio logserver.")
          .setIgnoredSiteProperty(true)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey LOGSERVER_PORT =
      new Builder(Name.LOGSERVER_PORT)
          .setDefaultValue(45600)
          .setDescription("Default port number to receive logs from alluxio servers.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey LOGSERVER_THREADS_MAX =
      new Builder(Name.LOGSERVER_THREADS_MAX)
          .setDefaultValue(2048)
          .setDescription("The maximum number of threads used by logserver to service"
              + " logging requests.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey LOGSERVER_THREADS_MIN =
      new Builder(Name.LOGSERVER_THREADS_MIN)
          .setDefaultValue(512)
          .setDescription("The minimum number of threads used by logserver to service"
              + " logging requests.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();

  //
  // User related properties
  //
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_THREADS =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_THREADS)
          .setDefaultValue(10)
          .setDescription("The number of threads used by a block master client pool to talk "
              + "to the block master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES)
          .setDefaultValue("8MB")
          .setDescription("The size of the file buffer to read data from remote Alluxio "
              + "worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_REPLICATION_MAX =
      new Builder(Name.USER_FILE_REPLICATION_MAX)
          .setDefaultValue(-1 /* negative value means infinity */)
          .build();
  public static final PropertyKey USER_FILE_REPLICATION_MIN =
      new Builder(Name.USER_FILE_REPLICATION_MIN).setDefaultValue(0).build();
  public static final PropertyKey USER_FILE_REPLICATION_DURABLE =
      new Builder(Name.USER_FILE_REPLICATION_DURABLE).setDefaultValue(1).build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_BLOCK_REMOTE_READER_CLASS =
      new Builder(Name.USER_BLOCK_REMOTE_READER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyRemoteBlockReader")
          .setDescription("Selects networking stack to run the client with. Currently only "
              + "`alluxio.client.netty.NettyRemoteBlockReader` (read remote data using netty) "
              + "is valid.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_BLOCK_REMOTE_WRITER_CLASS =
      new Builder(Name.USER_BLOCK_REMOTE_WRITER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyRemoteBlockWriter")
          .setDescription("Selects networking stack to run the client with for block writes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_SIZE_BYTES_DEFAULT =
      new Builder(Name.USER_BLOCK_SIZE_BYTES_DEFAULT)
          .setDefaultValue("512MB")
          .setDescription("Default block size for Alluxio files.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_THREADS =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_THREADS)
          .setDefaultValue(10)
          .setDescription("The number of threads used by a block worker client pool for "
              + "heartbeating to a worker. Increase this value if worker failures affect "
              + "client connections to healthy workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX)
          .setDefaultValue(128)
          .setDescription("The maximum number of block worker clients cached in the block "
              + "worker client pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setAlias(new String[]{"alluxio.user.block.worker.client.pool.gc.threshold.ms"})
          .setDefaultValue("300sec")
          .setDescription("A block worker client is closed if it has been idle for more than "
              + "this threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_READ_RETRY =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_READ_RETRY)
          .setDefaultValue(5)
          .setDescription("The maximum number of workers to retry before the client gives up on "
              + " reading a block")
          .build();
  public static final PropertyKey USER_CONF_CLUSTER_DEFAULT_ENABLED =
      new Builder(Name.USER_CONF_CLUSTER_DEFAULT_ENABLED)
          .setDefaultValue(true)
          .setDescription("When this property is true, an Alluxio client will load the default "
              + "values of configuration properties set by Alluxio master.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_SOCKET_TIMEOUT =
      new Builder(Name.USER_NETWORK_SOCKET_TIMEOUT)
          .setAlias(new String[]{
              "alluxio.security.authentication.socket.timeout",
              "alluxio.security.authentication.socket.timeout.ms"})
          .setDefaultValue("10min")
          .setDescription("The time out of a socket created by a user to connect to the master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_DATE_FORMAT_PATTERN =
      new Builder(Name.USER_DATE_FORMAT_PATTERN)
          .setDefaultValue("MM-dd-yyyy HH:mm:ss:SSS")
          .setDescription("Display formatted date in cli command and web UI by given date "
              + "format pattern.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FAILED_SPACE_REQUEST_LIMITS =
      new Builder(Name.USER_FAILED_SPACE_REQUEST_LIMITS)
          .setDefaultValue(3)
          .setDescription("The number of times to request space from the file system before "
              + "aborting.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_BUFFER_BYTES =
      new Builder(Name.USER_FILE_BUFFER_BYTES)
          .setDefaultValue("8MB")
          .setDescription("The size of the file buffer to use for file system reads/writes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
      new Builder(Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK)
          .setDefaultValue(true)
          .setDescription("This property is deprecated as of 1.7 and has no effect. Use the read "
              + "type to control caching behavior.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_COPY_FROM_LOCAL_WRITE_LOCATION_POLICY =
      new Builder(Name.USER_FILE_COPY_FROM_LOCAL_WRITE_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.RoundRobinPolicy")
          .setDescription("The default location policy for choosing workers for writing a "
              + "file's blocks using copyFromLocal command.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_DELETE_UNCHECKED =
      new Builder(Name.USER_FILE_DELETE_UNCHECKED)
          .setDefaultValue(false)
          .setDescription("Whether to check if the UFS contents are in sync with Alluxio "
              + "before attempting to delete persisted directories recursively.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_MASTER_CLIENT_THREADS =
      new Builder(Name.USER_FILE_MASTER_CLIENT_THREADS)
          .setDefaultValue(10)
          .setDescription("The number of threads used by a file master client to talk to the "
              + "file master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_METADATA_LOAD_TYPE =
      new Builder(Name.USER_FILE_METADATA_LOAD_TYPE)
          .setDefaultValue("Once")
          .setDescription("The behavior of loading metadata from UFS. When information about "
              + "a path is requested and the path does not exist in Alluxio, metadata can be "
              + "loaded from the UFS. Valid options are `Always`, `Never`, and `Once`. "
              + "`Always` will always access UFS to see if the path exists in the UFS. "
              + "`Never` will never consult the UFS. `Once` will access the UFS the \"first\" "
              + "time (according to a cache), but not after that. This parameter is ignored if a "
              + "metadata sync is performed, via the parameter "
              + "\"alluxio.user.file.metadata.sync.interval\"")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_METADATA_SYNC_INTERVAL =
      new Builder(Name.USER_FILE_METADATA_SYNC_INTERVAL)
          .setDefaultValue("-1")
          .setDescription("The interval for syncing UFS metadata before invoking an "
              + "operation on a path. -1 means no sync will occur. 0 means Alluxio will "
              + "always sync the metadata of the path before an operation. If you specify a time "
              + "interval, Alluxio will (best effort) not re-sync a path within that time "
              + "interval. Syncing the metadata for a path must interact with the UFS, so it is "
              + "an expensive operation. If a sync is performed for an operation, the "
              + "configuration of \"alluxio.user.file.metadata.load.type\" will be ignored.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_PASSIVE_CACHE_ENABLED =
      new Builder(Name.USER_FILE_PASSIVE_CACHE_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to cache files to local Alluxio workers when the files are read "
              + "from remote workers (not UFS).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_LOAD_TTL =
      new Builder(Name.USER_FILE_LOAD_TTL)
          .setDefaultValue(Constants.NO_TTL)
          .setDescription("Time to live for files loaded from UFS by a user, no ttl by default.")
          .build();
  public static final PropertyKey USER_FILE_LOAD_TTL_ACTION =
      new Builder(Name.USER_FILE_LOAD_TTL_ACTION)
          .setDefaultValue("FREE")
          .setDescription("When file's ttl is expired, the action performs on it. "
              + "FREE by default")
          .build();
  public static final PropertyKey USER_FILE_READ_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_READ_TYPE_DEFAULT)
          .setDefaultValue("CACHE_PROMOTE")
          .setDescription("Default read type when creating Alluxio files. Valid options are "
              + "`CACHE_PROMOTE` (move data to highest tier if already in Alluxio storage, "
              + "write data into highest tier of local Alluxio if data needs to be read from "
              + "under storage), `CACHE` (write data into highest tier of local Alluxio if "
              + "data needs to be read from under storage), `NO_CACHE` (no data interaction "
              + "with Alluxio, if the read is from Alluxio data migration or eviction will "
              + "not occur).")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_SEEK_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES)
          .setDefaultValue("1MB")
          .setDescription("The file seek buffer size. This is only used when "
              + "alluxio.user.file.cache.partially.read.block is enabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WAITCOMPLETED_POLL_MS =
      new Builder(Name.USER_FILE_WAITCOMPLETED_POLL_MS)
          .setAlias(new String[]{"alluxio.user.file.waitcompleted.poll.ms"})
          .setDefaultValue("1sec")
          .setDescription("The time interval to poll a file for its completion status when "
              + "using waitCompleted.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_CREATE_TTL =
      new Builder(Name.USER_FILE_CREATE_TTL)
          .setDefaultValue(Constants.NO_TTL)
          .setDescription("Time to live for files created by a user, no ttl by default.")
          .build();
  public static final PropertyKey USER_FILE_CREATE_TTL_ACTION =
      new Builder(Name.USER_FILE_CREATE_TTL_ACTION)
          .setDefaultValue("DELETE")
          .setDescription("When file's ttl is expired, the action performs on it. "
              + "DELETE by default")
          .build();
  public static final PropertyKey USER_FILE_UFS_TIER_ENABLED =
      new Builder(Name.USER_FILE_UFS_TIER_ENABLED)
          .setDescription("When workers run out of available memory, whether the client can skip "
              + "writing data to Alluxio but fallback to write to UFS without stopping the "
              + "application. This property only works when the write type is ASYNC_THROUGH.")
          .setDefaultValue(false).build();
  public static final PropertyKey USER_FILE_WRITE_LOCATION_POLICY =
      new Builder(Name.USER_FILE_WRITE_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.LocalFirstPolicy")
          .setDescription("The default location policy for choosing workers for writing a "
              + "file's blocks.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
      new Builder(Name.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES)
          .setDefaultValue("0MB")
          .setDescription("The portion of space reserved in worker when user use the "
              + "LocalFirstAvoidEvictionPolicy class as file write location policy.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WRITE_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_WRITE_TYPE_DEFAULT)
          .setDefaultValue("MUST_CACHE")
          .setDescription("Default write type when creating Alluxio files. Valid options are "
              + "`MUST_CACHE` (write will only go to Alluxio and must be stored in Alluxio), "
              + "`CACHE_THROUGH` (try to cache, write to UnderFS synchronously), `THROUGH` "
              + "(no cache, write to UnderFS synchronously).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WRITE_TIER_DEFAULT =
      new Builder(Name.USER_FILE_WRITE_TIER_DEFAULT)
          .setDefaultValue(Constants.FIRST_TIER)
          .setDescription("The default tier for choosing a where to write a block. Valid "
              + "option is any integer. Non-negative values identify tiers starting from top "
              + "going down (0 identifies the first tier, 1 identifies the second tier, and "
              + "so on). If the provided value is greater than the number of tiers, it "
              + "identifies the last tier. Negative values identify tiers starting from the "
              + "bottom going up (-1 identifies the last tier, -2 identifies the second to "
              + "last tier, and so on). If the absolute value of the provided value is "
              + "greater than the number of tiers, it identifies the first tier.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.USER_HEARTBEAT_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.user.heartbeat.interval.ms"})
          .setDefaultValue("1sec")
          .setDescription("The interval between Alluxio workers' heartbeats.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link PropertyKey#LOCALITY_TIER_NODE} instead
   */
  @Deprecated
  public static final PropertyKey USER_HOSTNAME = new Builder(Name.USER_HOSTNAME)
      .setDescription(String.format("The hostname to use for the client. Note: this property is "
          + "deprecated. set %s instead", PropertyKey.LOCALITY_TIER_NODE.toString()))
      .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
      .setScope(Scope.CLIENT)
      .build();
  public static final PropertyKey USER_LOCAL_READER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_READER_PACKET_SIZE_BYTES)
          .setDefaultValue("8MB")
          .setDescription("When a client reads from a local worker, the maximum data packet size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_WRITER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .setDescription("When a client writes to a local worker, the maximum data packet size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METRICS_COLLECTION_ENABLED =
      new Builder(Name.USER_METRICS_COLLECTION_ENABLED)
          .setDefaultValue(true)
          .setDescription("Enable collecting the client-side metrics and hearbeat them to master")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METRICS_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.USER_METRICS_HEARTBEAT_INTERVAL_MS)
          .setAlias(new String[]{"alluxio.user.heartbeat.interval.ms"})
          .setDefaultValue("3sec")
          .setDescription("The time period of client master hearbeat to "
              + "send the client-side metrics.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_APP_ID =
      new Builder(Name.USER_APP_ID)
          .setScope(Scope.CLIENT)
          .setDescription("The custom id to use for labeling this client's info, such as metrics. "
              + "If unset, a random long will be used. This value is displayed in the client logs "
              + "on initialization. Note that using the same app id will cause client info to be "
              + "aggregated, so different applications must set their own ids or leave this value "
              + "unset to use a randomly generated id.")
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL)
          .setDescription("Type of netty channels. If EPOLL is not available, this will "
              + "automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setDefaultValue("EPOLL")
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_NETTY_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.user.network.netty.timeout.ms"})
          .setDefaultValue("30sec")
          .setDescription("The maximum time for a netty client (for block "
              + "reads and block writes) to wait for a response from the data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WORKER_THREADS =
      new Builder(Name.USER_NETWORK_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .setDescription("How many threads to use for remote block worker client to read "
              + "from remote block workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of netty channels cached in the netty channel "
              + "pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS)
          .setAlias(new String[]{"alluxio.user.network.netty.channel.pool.gc.threshold.ms"})
          .setDefaultValue("300sec")
          .setDescription("A netty channel is closed if it has been idle for more than this "
              + "threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL_POOL_DISABLED)
          .setDefaultValue(false)
          .setDescription("Disable netty channel pool. This should be turned on if the client "
              + "version is >= 1.3.0 but server version is <= 1.2.x.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .setDescription("When a client writes to a remote worker, the maximum packet size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .setDescription("When a client writes to a remote worker, the maximum number of packets "
              + "to buffer by the client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.user.network.netty.writer.close.timeout.ms"})
          .setDefaultValue("30min")
          .setDescription("The timeout to close a netty writer client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_WRITER_FLUSH_TIMEOUT =
      new Builder(Name.USER_NETWORK_NETTY_WRITER_FLUSH_TIMEOUT)
          .setDefaultValue("30min")
          .setDescription("The timeout to wait for flush to finish in a netty writer.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS =
      new Builder(Name.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS)
          .setDefaultValue(16)
          .setDescription("When a client reads from a remote worker, the maximum number of packets "
              + "to buffer by the client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES)
          .setDefaultValue("64KB")
          .setDescription("When a client reads from a remote worker, the maximum packet size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_RPC_RETRY_BASE_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_BASE_SLEEP_MS)
          .setAlias(new String[]{"alluxio.user.rpc.retry.base.sleep.ms"})
          .setDefaultValue("50ms")
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the base time "
              + "in the exponential backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_RPC_RETRY_MAX_DURATION =
      new Builder(Name.USER_RPC_RETRY_MAX_DURATION)
          .setDefaultValue("2min")
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the maximum duration to retry for"
              + " before giving up. Note that, this value is set to 5s for fs and fsadmin CLIs.")
          .build();
  public static final PropertyKey USER_WORKER_LIST_REFRESH_INTERVAL =
      new Builder(Name.USER_WORKER_LIST_REFRESH_INTERVAL)
          .setDefaultValue("2min")
          .setDescription("The interval used to refresh the live worker list on the client")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated since version 1.8 and will be removed in version 2.0.
   */
  @Deprecated
  public static final PropertyKey USER_RPC_RETRY_MAX_NUM_RETRY =
      new Builder(Name.USER_RPC_RETRY_MAX_NUM_RETRY)
          .setAlias(new String[]{Name.MASTER_RETRY})
          .setDefaultValue(100)
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the maximum number of "
              + "retries. This property has been deprecated by time-based retry using: "
              + Name.USER_RPC_RETRY_MAX_DURATION)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_RPC_RETRY_MAX_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_MAX_SLEEP_MS)
          .setAlias(new String[]{"alluxio.user.rpc.retry.max.sleep.ms"})
          .setDefaultValue("3sec")
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the maximum wait time "
              + "in the backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES)
          .setDefaultValue("8MB")
          .setDescription("Size of the read buffer when reading from the UFS through the "
              + "Alluxio worker. Each read request will fetch at least this many bytes, "
              + "unless the read reaches the end of the file.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
      new Builder(Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES)
          .setDefaultValue("2MB")
          .setDescription("Size of the write buffer when writing to the UFS through the "
              + "Alluxio worker. Each write request will write at least this many bytes, "
              + "unless the write is at the end of the file.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_FILE_READER_CLASS =
      new Builder(Name.USER_UFS_FILE_READER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyUnderFileSystemFileReader")
          .setDescription("Selects networking stack to run the client with for reading from "
              + "under file system through a worker's data server. Currently only "
              + "`alluxio.client.netty.NettyUnderFileSystemFileReader` (remote read using "
              + "netty) is valid.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated It will be removed in 2.0.0.
   */
  @Deprecated
  public static final PropertyKey USER_UFS_FILE_WRITER_CLASS =
      new Builder(Name.USER_UFS_FILE_WRITER_CLASS)
          .setDefaultValue("alluxio.client.netty.NettyUnderFileSystemFileWriter")
          .setDescription("Selects networking stack to run the client with for writing to "
              + "under file system through a worker's data server. Currently only "
              + "`alluxio.client.netty.NettyUnderFileSystemFileWriter` (remote write using "
              + "netty) is valid.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED =
      new Builder(Name.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to return all workers as block location if ufs block locations "
              + "are not co-located with any Alluxio workers or is empty.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY =
      new Builder(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.file.policy.LocalFirstPolicy")
          .setDescription(String.format("When an Alluxio client reads a file from the UFS, it "
              + "delegates the read to an Alluxio worker. The client uses this policy to choose "
              + "which worker to read through. Builtin choices: %s.", Arrays.asList(
              javadocLink("alluxio.client.block.policy.DeterministicHashPolicy"),
              javadocLink("alluxio.client.file.policy.LocalFirstAvoidEvictionPolicy"),
              javadocLink("alluxio.client.file.policy.LocalFirstPolicy"),
              javadocLink("alluxio.client.file.policy.MostAvailableFirstPolicy"),
              javadocLink("alluxio.client.file.policy.RoundRobinPolicy"),
              javadocLink("alluxio.client.file.policy.SpecificHostPolicy"))))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
      new Builder(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS)
          .setDefaultValue(1)
          .setDescription("When alluxio.user.ufs.block.read.location.policy is set to "
              + "alluxio.client.block.policy.DeterministicHashPolicy, this specifies the number of "
              + "hash shards.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
      new Builder(Name.USER_UFS_BLOCK_READ_CONCURRENCY_MAX)
          .setDefaultValue(Integer.MAX_VALUE)
          .setDescription("The maximum concurrent readers for one UFS block on one Block Worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_SHORT_CIRCUIT_ENABLED =
      new Builder(Name.USER_SHORT_CIRCUIT_ENABLED)
          .setDefaultValue(true)
          .setDescription("The short circuit read/write which allows the clients to "
              + "read/write data without going through Alluxio workers if the data is local "
              + "is enabled if set to true.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();

  //
  // FUSE integration related properties
  //
  public static final PropertyKey FUSE_CACHED_PATHS_MAX =
      new Builder(Name.FUSE_CACHED_PATHS_MAX)
          .setDefaultValue(500)
          .setDescription("Maximum number of Alluxio paths to cache for FUSE conversion.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey FUSE_DEBUG_ENABLED =
      new Builder(Name.FUSE_DEBUG_ENABLED)
          .setDefaultValue(false)
          .setDescription("Run FUSE in debug mode, and have the fuse process log every FS request.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey FUSE_FS_NAME =
      new Builder(Name.FUSE_FS_NAME)
          .setDefaultValue("alluxio-fuse")
          .setDescription("The FUSE file system name.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey FUSE_MAXWRITE_BYTES =
      new Builder(Name.FUSE_MAXWRITE_BYTES)
          .setDefaultValue("128KB")
          .setDescription("Maximum granularity of write operations, capped by the kernel to 128KB "
              + "max (as of Linux 3.16.0).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();

  //
  // Security related properties
  //
  public static final PropertyKey SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
      new Builder(Name.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS)
          .setDescription("The class to provide customized authentication implementation, "
              + "when alluxio.security.authentication.type is set to CUSTOM. It must "
              + "implement the interface "
              + "'alluxio.security.authentication.AuthenticationProvider'.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey SECURITY_AUTHENTICATION_TYPE =
      new Builder(Name.SECURITY_AUTHENTICATION_TYPE)
          .setDefaultValue("SIMPLE")
          .setDescription("The authentication mode. Currently three modes are supported: "
              + "NOSASL, SIMPLE, CUSTOM. The default value SIMPLE indicates that a simple "
              + "authentication is enabled. Server trusts whoever the client claims to be.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to enable access control based on file permission.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP)
          .setDefaultValue("supergroup")
          .setDescription("The super group of Alluxio file system. All users in this group "
              + "have super permission.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_UMASK =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_UMASK)
          .setDefaultValue("022")
          .setDescription("The umask of creating file and directory. The initial creation "
              + "permission is 777, and the difference between directory and file is 111. So "
              + "for default umask value 022, the created directory has permission 755 and "
              + "file has permission 644.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
      new Builder(Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.security.group.mapping.cache.timeout.ms"})
          .setDefaultValue("1min")
          .setDescription("Time for cached group mapping to expire.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_CLASS =
      new Builder(Name.SECURITY_GROUP_MAPPING_CLASS)
          .setDefaultValue("alluxio.security.group.provider.ShellBasedUnixGroupsMapping")
          .setDescription("The class to provide user-to-groups mapping service. Master could "
              + "get the various group memberships of a given user.  It must implement the "
              + "interface 'alluxio.security.group.GroupMappingService'. The default "
              + "implementation execute the 'groups' shell command to fetch the group "
              + "memberships of a given user.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_LOGIN_IMPERSONATION_USERNAME =
      new Builder(Name.SECURITY_LOGIN_IMPERSONATION_USERNAME).setDescription(String.format(
          "When %s is set to SIMPLE or CUSTOM, user application uses this property to indicate "
              + "the IMPERSONATED user requesting Alluxio service. If it is not set explicitly, "
              + "or set to %s, impersonation will not be used. A special value of '%s' can be "
              + "specified to impersonate the hadoop client user.", SECURITY_AUTHENTICATION_TYPE,
          Constants.IMPERSONATION_NONE, Constants.IMPERSONATION_HDFS_USER))
          .setDefaultValue(Constants.IMPERSONATION_HDFS_USER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey SECURITY_LOGIN_USERNAME =
      new Builder(Name.SECURITY_LOGIN_USERNAME)
          .setDescription("When alluxio.security.authentication.type is set to SIMPLE or "
              + "CUSTOM, user application uses this property to indicate the user requesting "
              + "Alluxio service. If it is not set explicitly, the OS login user will be used.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.CLIENT)
          .build();

  //
  // Mesos and Yarn related properties
  //
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_CPU =
      new Builder(Name.INTEGRATION_MASTER_RESOURCE_CPU)
          .setDefaultValue(1)
          .setDescription("The number of CPUs to run an Alluxio master for YARN framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MASTER_RESOURCE_MEM =
      new Builder(Name.INTEGRATION_MASTER_RESOURCE_MEM)
          .setDefaultValue("1024MB")
          .setDescription("The amount of memory to run an Alluxio master for YARN framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_JAR_URL =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_JAR_URL)
          .setDefaultValue(String.format(
              "http://downloads.alluxio.org/downloads/files/${%s}/alluxio-${%s}-bin.tar.gz",
              Name.VERSION, Name.VERSION))
          .setDescription("Url to download an Alluxio distribution from during Mesos deployment.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NAME =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME)
          .setDefaultValue("AlluxioMaster")
          .setDescription("The name of the master process to use within Mesos.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT)
          .setDescription("The number of Alluxio master process to run within Mesos.")
          .setDefaultValue(1)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ALLUXIO_WORKER_NAME =
      new Builder(Name.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME)
          .setDefaultValue("AlluxioWorker")
          .setDescription("The name of the worker process to use within Mesos.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_JDK_PATH =
      new Builder(Name.INTEGRATION_MESOS_JDK_PATH)
          .setDefaultValue("jdk1.8.0_151")
          .setDescription("If installing java from a remote URL during mesos deployment, this must "
              + "be set to the directory name of the untarred jdk.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_JDK_URL =
      new Builder(Name.INTEGRATION_MESOS_JDK_URL)
          .setDefaultValue(Constants.MESOS_LOCAL_INSTALL)
          .setDescription("A url from which to install the jdk during Mesos deployment. Default to "
              + "LOCAL which tells Mesos to use the local JDK on the system. When "
              + "using this property, alluxio.integration.mesos.jdk.path must also be set "
              + "correctly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_PRINCIPAL =
      new Builder(Name.INTEGRATION_MESOS_PRINCIPAL)
          .setDefaultValue("alluxio")
          .setDescription("The Mesos principal for the Alluxio Mesos Framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_ROLE =
      new Builder(Name.INTEGRATION_MESOS_ROLE)
          .setDefaultValue("*")
          .setDescription("Mesos role for the Alluxio Mesos Framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_SECRET =
      new Builder(Name.INTEGRATION_MESOS_SECRET)
          .setDescription("Secret token for authenticating with Mesos.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.NONE)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey INTEGRATION_MESOS_USER =
      new Builder(Name.INTEGRATION_MESOS_USER)
          .setDescription("The Mesos user for the Alluxio Mesos Framework. Defaults to the current "
              + "user.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_CPU =
      new Builder(Name.INTEGRATION_WORKER_RESOURCE_CPU)
          .setDefaultValue(1)
          .setDescription("The number of CPUs to run an Alluxio worker for YARN framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_WORKER_RESOURCE_MEM =
      new Builder(Name.INTEGRATION_WORKER_RESOURCE_MEM)
          .setDefaultValue("1024MB")
          .setDescription("The amount of memory to run an Alluxio worker for YARN framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey INTEGRATION_YARN_WORKERS_PER_HOST_MAX =
      new Builder(Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX)
          .setDefaultValue(1)
          .setDescription("The number of workers to run on an Alluxio host for YARN framework.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.NONE)
          .build();

  //
  // Job service
  //
  public static final PropertyKey JOB_MASTER_CLIENT_THREADS =
      new Builder(Name.JOB_MASTER_CLIENT_THREADS).setDefaultValue(1024).build();
  public static final PropertyKey JOB_MASTER_FINISHED_JOB_RETENTION_MS =
      new Builder(Name.JOB_MASTER_FINISHED_JOB_RETENTION_MS).setDefaultValue(300000).build();
  public static final PropertyKey JOB_MASTER_JOB_CAPACITY =
      new Builder(Name.JOB_MASTER_JOB_CAPACITY).setDefaultValue(100000).build();
  public static final PropertyKey JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS).setDefaultValue(1000).build();
  public static final PropertyKey JOB_MASTER_WORKER_TIMEOUT_MS =
      new Builder(Name.JOB_MASTER_WORKER_TIMEOUT_MS).setDefaultValue(60000).build();
  public static final PropertyKey JOB_MASTER_BIND_HOST =
      new Builder(Name.JOB_MASTER_BIND_HOST).setDefaultValue("0.0.0.0").build();
  public static final PropertyKey JOB_MASTER_HOSTNAME =
      new Builder(Name.JOB_MASTER_HOSTNAME).setDefaultValue("${alluxio.master.hostname}").build();
  public static final PropertyKey JOB_MASTER_LOST_WORKER_INTERVAL_MS =
      new Builder(Name.JOB_MASTER_LOST_WORKER_INTERVAL_MS).setDefaultValue(1000).build();
  public static final PropertyKey JOB_MASTER_RPC_PORT =
      new Builder(Name.JOB_MASTER_RPC_PORT).setDefaultValue(20001).build();
  public static final PropertyKey JOB_MASTER_WEB_BIND_HOST =
      new Builder(Name.JOB_MASTER_WEB_BIND_HOST).setDefaultValue("0.0.0.0").build();
  public static final PropertyKey JOB_MASTER_WEB_HOSTNAME =
      new Builder(Name.JOB_MASTER_WEB_HOSTNAME)
          .setDefaultValue("${alluxio.job.master.hostname}")
          .build();
  public static final PropertyKey JOB_MASTER_WEB_PORT =
      new Builder(Name.JOB_MASTER_WEB_PORT).setDefaultValue(20002).build();
  public static final PropertyKey JOB_WORKER_BIND_HOST =
      new Builder(Name.JOB_WORKER_BIND_HOST).setDefaultValue("0.0.0.0").build();
  public static final PropertyKey JOB_WORKER_DATA_PORT =
      new Builder(Name.JOB_WORKER_DATA_PORT).setDefaultValue(30002).build();
  public static final PropertyKey JOB_WORKER_RPC_PORT =
      new Builder(Name.JOB_WORKER_RPC_PORT).setDefaultValue(30001).build();
  public static final PropertyKey JOB_WORKER_WEB_BIND_HOST =
      new Builder(Name.JOB_WORKER_WEB_BIND_HOST).setDefaultValue("0.0.0.0").build();
  public static final PropertyKey JOB_WORKER_WEB_PORT =
      new Builder(Name.JOB_WORKER_WEB_PORT).setDefaultValue(30003).build();

  public static final PropertyKey ZOOKEEPER_JOB_ELECTION_PATH =
      new Builder(Name.ZOOKEEPER_JOB_ELECTION_PATH).setDefaultValue("/job_election").build();
  public static final PropertyKey ZOOKEEPER_JOB_LEADER_PATH =
      new Builder(Name.ZOOKEEPER_JOB_LEADER_PATH).setDefaultValue("/job_leader").build();

  //
  // JVM Monitor related properties
  //
  public static final PropertyKey JVM_MONITOR_WARN_THRESHOLD_MS =
      new Builder(Name.JVM_MONITOR_WARN_THRESHOLD_MS)
          .setDefaultValue("10sec")
          .setDescription("Extra sleep time longer than this threshold, log WARN.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey JVM_MONITOR_INFO_THRESHOLD_MS =
      new Builder(Name.JVM_MONITOR_INFO_THRESHOLD_MS)
          .setDefaultValue("1sec")
          .setDescription("Extra sleep time longer than this threshold, log INFO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey JVM_MONITOR_SLEEP_INTERVAL_MS =
      new Builder(Name.JVM_MONITOR_SLEEP_INTERVAL_MS)
          .setDefaultValue("1sec")
          .setDescription("The time for the JVM monitor thread to sleep.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey MASTER_JVM_MONITOR_ENABLED =
      new Builder(Name.MASTER_JVM_MONITOR_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to enable start JVM monitor thread on master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey WORKER_JVM_MONITOR_ENABLED =
      new Builder(Name.WORKER_JVM_MONITOR_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to enable start JVM monitor thread on worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();

  /**
   * @param fullyQualifiedClassname a fully qualified classname
   * @return html linking the text of the classname to the alluxio javadoc for the class
   */
  private static String javadocLink(String fullyQualifiedClassname) {
    String javadocPath = fullyQualifiedClassname.replace(".", "/") + ".html";
    return String.format("<a href=\"%s\">%s</a>",
        PathUtils.concatPath(RuntimeConstants.ALLUXIO_JAVADOC_URL, javadocPath),
        fullyQualifiedClassname);
  }

  /**
   * A nested class to hold named string constants for their corresponding properties.
   * Used for setting configuration in integration tests.
   */
  @ThreadSafe
  public static final class Name {
    public static final String CONF_DIR = "alluxio.conf.dir";
    public static final String CONF_VALIDATION_ENABLED = "alluxio.conf.validation.enabled";
    public static final String DEBUG = "alluxio.debug";
    public static final String EXTENSIONS_DIR = "alluxio.extensions.dir";
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
    public static final String METRICS_CONTEXT_SHUTDOWN_TIMEOUT =
        "alluxio.metrics.context.shutdown.timeout";
    public static final String NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
        "alluxio.network.host.resolution.timeout";
    public static final String NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS =
        "alluxio.network.netty.heartbeat.timeout";
    public static final String NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX =
        "alluxio.network.thrift.frame.size.bytes.max";
    public static final String SITE_CONF_DIR = "alluxio.site.conf.dir";
    public static final String TEST_MODE = "alluxio.test.mode";
    public static final String TMP_DIRS = "alluxio.tmp.dirs";
    public static final String VERSION = "alluxio.version";
    public static final String WEB_FILE_INFO_ENABLED = "alluxio.web.file.info.enabled";
    public static final String WEB_RESOURCES = "alluxio.web.resources";
    public static final String WEB_TEMP_PATH = "alluxio.web.temp.path";
    public static final String WEB_THREADS = "alluxio.web.threads";
    public static final String WORK_DIR = "alluxio.work.dir";
    public static final String ZOOKEEPER_ADDRESS = "alluxio.zookeeper.address";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT =
        "alluxio.zookeeper.connection.timeout";
    public static final String ZOOKEEPER_ELECTION_PATH = "alluxio.zookeeper.election.path";
    public static final String ZOOKEEPER_ENABLED = "alluxio.zookeeper.enabled";
    public static final String ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
        "alluxio.zookeeper.leader.inquiry.retry";
    public static final String ZOOKEEPER_LEADER_PATH = "alluxio.zookeeper.leader.path";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "alluxio.zookeeper.session.timeout";

    //
    // UFS related properties
    //
    public static final String UNDERFS_ADDRESS = "alluxio.underfs.address";
    public static final String UNDERFS_ALLOW_SET_OWNER_FAILURE =
        "alluxio.underfs.allow.set.owner.failure";
    public static final String UNDERFS_CLEANUP_ENABLED = "alluxio.underfs.cleanup.enabled";
    public static final String UNDERFS_CLEANUP_INTERVAL = "alluxio.underfs.cleanup.interval";
    public static final String UNDERFS_LISTING_LENGTH = "alluxio.underfs.listing.length";
    public static final String UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
        "alluxio.underfs.gcs.owner.id.to.username.mapping";
    public static final String UNDERFS_HDFS_CONFIGURATION = "alluxio.underfs.hdfs.configuration";
    public static final String UNDERFS_HDFS_IMPL = "alluxio.underfs.hdfs.impl";
    public static final String UNDERFS_HDFS_PREFIXES = "alluxio.underfs.hdfs.prefixes";
    public static final String UNDERFS_HDFS_REMOTE = "alluxio.underfs.hdfs.remote";
    public static final String UNDERFS_OBJECT_STORE_SERVICE_THREADS =
        "alluxio.underfs.object.store.service.threads";
    public static final String UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
        "alluxio.underfs.object.store.mount.shared.publicly";
    public static final String UNDERFS_OBJECT_STORE_READ_RETRY_BASE_SLEEP_MS =
        "alluxio.underfs.object.store.read.retry.base.sleep";
    public static final String UNDERFS_OBJECT_STORE_READ_RETRY_MAX_NUM =
        "alluxio.underfs.object.store.read.retry.max.num";
    public static final String UNDERFS_OBJECT_STORE_READ_RETRY_MAX_SLEEP_MS =
        "alluxio.underfs.object.store.read.retry.max.sleep";
    public static final String UNDERFS_OSS_CONNECT_MAX = "alluxio.underfs.oss.connection.max";
    public static final String UNDERFS_OSS_CONNECT_TIMEOUT =
        "alluxio.underfs.oss.connection.timeout";
    public static final String UNDERFS_OSS_CONNECT_TTL = "alluxio.underfs.oss.connection.ttl";
    public static final String UNDERFS_OSS_SOCKET_TIMEOUT = "alluxio.underfs.oss.socket.timeout";
    public static final String UNDERFS_S3A_BULK_DELETE_ENABLED =
        "alluxio.underfs.s3a.bulk.delete.enabled";
    public static final String UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
        "alluxio.underfs.s3a.consistency.timeout";
    public static final String UNDERFS_S3A_DIRECTORY_SUFFIX =
        "alluxio.underfs.s3a.directory.suffix";
    public static final String UNDERFS_S3A_INHERIT_ACL = "alluxio.underfs.s3a.inherit_acl";
    public static final String UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE =
        "alluxio.underfs.s3a.intermediate.upload.clean.age";
    public static final String UNDERFS_S3A_LIST_OBJECTS_VERSION_1 =
        "alluxio.underfs.s3a.list.objects.v1";
    public static final String UNDERFS_S3A_REQUEST_TIMEOUT_MS =
        "alluxio.underfs.s3a.request.timeout";
    public static final String UNDERFS_S3A_SECURE_HTTP_ENABLED =
        "alluxio.underfs.s3a.secure.http.enabled";
    public static final String UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED =
        "alluxio.underfs.s3a.server.side.encryption.enabled";
    public static final String UNDERFS_S3A_SIGNER_ALGORITHM =
        "alluxio.underfs.s3a.signer.algorithm";
    public static final String UNDERFS_S3A_SOCKET_TIMEOUT_MS =
        "alluxio.underfs.s3a.socket.timeout";
    public static final String UNDERFS_S3A_STREAMING_UPLOAD_ENABLED =
        "alluxio.underfs.s3a.streaming.upload.enabled";
    public static final String UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE =
        "alluxio.underfs.s3a.streaming.upload.partition.size";
    public static final String UNDERFS_S3_ADMIN_THREADS_MAX =
        "alluxio.underfs.s3.admin.threads.max";
    public static final String UNDERFS_S3_DISABLE_DNS_BUCKETS =
        "alluxio.underfs.s3.disable.dns.buckets";
    public static final String UNDERFS_S3_ENDPOINT = "alluxio.underfs.s3.endpoint";
    public static final String UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING =
        "alluxio.underfs.s3.owner.id.to.username.mapping";
    public static final String UNDERFS_S3_PROXY_HOST = "alluxio.underfs.s3.proxy.host";
    public static final String UNDERFS_S3_PROXY_PORT = "alluxio.underfs.s3.proxy.port";
    public static final String UNDERFS_S3_THREADS_MAX = "alluxio.underfs.s3.threads.max";
    public static final String UNDERFS_S3_UPLOAD_THREADS_MAX =
        "alluxio.underfs.s3.upload.threads.max";

    //
    // UFS access control related properties
    //
    public static final String COS_ACCESS_KEY = "fs.cos.access.key";
    public static final String COS_APP_ID = "fs.cos.app.id";
    public static final String COS_CONNECTION_MAX = "fs.cos.connection.max";
    public static final String COS_CONNECTION_TIMEOUT = "fs.cos.connection.timeout";
    public static final String COS_REGION = "fs.cos.region";
    public static final String COS_SECRET_KEY = "fs.cos.secret.key";
    public static final String COS_SOCKET_TIMEOUT = "fs.cos.socket.timeout";
    public static final String GCS_ACCESS_KEY = "fs.gcs.accessKeyId";
    public static final String GCS_SECRET_KEY = "fs.gcs.secretAccessKey";
    public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    public static final String OSS_ENDPOINT_KEY = "fs.oss.endpoint";
    public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    public static final String S3A_ACCESS_KEY = "aws.accessKeyId";
    public static final String S3A_SECRET_KEY = "aws.secretKey";
    public static final String SWIFT_API_KEY = "fs.swift.apikey";
    public static final String SWIFT_AUTH_METHOD_KEY = "fs.swift.auth.method";
    public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
    public static final String SWIFT_PASSWORD_KEY = "fs.swift.password";
    public static final String SWIFT_REGION_KEY = "fs.swift.region";
    public static final String SWIFT_SIMULATION = "fs.swift.simulation";
    public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
    public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";
    public static final String SWIFT_USER_KEY = "fs.swift.user";

    //
    // Master related properties
    //
    public static final String MASTER_AUDIT_LOGGING_ENABLED =
        "alluxio.master.audit.logging.enabled";
    public static final String MASTER_AUDIT_LOGGING_QUEUE_CAPACITY =
        "alluxio.master.audit.logging.queue.capacity";
    public static final String MASTER_BACKUP_DIRECTORY =
        "alluxio.master.backup.directory";
    public static final String MASTER_BIND_HOST = "alluxio.master.bind.host";
    public static final String MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL =
        "alluxio.master.client.socket.cleanup.interval";
    public static final String MASTER_CLUSTER_METRICS_UPDATE_INTERVAL =
        "alluxio.master.cluster.metrics.update.interval";
    public static final String MASTER_CONNECTION_TIMEOUT_MS =
        "alluxio.master.connection.timeout";
    public static final String MASTER_FILE_ASYNC_PERSIST_HANDLER =
        "alluxio.master.file.async.persist.handler";
    public static final String MASTER_FORMAT_FILE_PREFIX = "alluxio.master.format.file_prefix";
    public static final String MASTER_MASTER_HEARTBEAT_INTERVAL =
        "alluxio.master.master.heartbeat.interval";
    public static final String MASTER_WORKER_HEARTBEAT_INTERVAL =
        "alluxio.master.worker.heartbeat.interval";
    public static final String MASTER_HEARTBEAT_TIMEOUT =
        "alluxio.master.heartbeat.timeout";
    public static final String MASTER_HOSTNAME = "alluxio.master.hostname";
    public static final String MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
        "alluxio.master.journal.flush.batch.time";
    public static final String MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
        "alluxio.master.journal.flush.timeout";
    public static final String MASTER_JOURNAL_FLUSH_RETRY_INTERVAL =
        "alluxio.master.journal.retry.interval";
    public static final String MASTER_JOURNAL_FOLDER = "alluxio.master.journal.folder";
    public static final String MASTER_JOURNAL_INIT_FROM_BACKUP =
        "alluxio.master.journal.init.from.backup";
    public static final String MASTER_JOURNAL_TYPE = "alluxio.master.journal.type";
    public static final String MASTER_JOURNAL_FORMATTER_CLASS =
        "alluxio.master.journal.formatter.class";
    public static final String MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
        "alluxio.master.journal.log.size.bytes.max";
    public static final String MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
        "alluxio.master.journal.tailer.shutdown.quiet.wait.time";
    public static final String MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
        "alluxio.master.journal.tailer.sleep.time";
    public static final String MASTER_KEYTAB_KEY_FILE = "alluxio.master.keytab.file";
    public static final String MASTER_PERSISTENCE_CHECKER_INTERVAL_MS =
        "alluxio.master.persistence.checker.interval.ms";
    public static final String MASTER_PERSISTENCE_INITIAL_INTERVAL_MS =
        "alluxio.master.persistence.initial.interval.ms";
    public static final String MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS =
        "alluxio.master.persistence.initial.wait.time.ms";
    public static final String MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS =
        "alluxio.master.persistence.max.total.wait.time.ms";
    public static final String MASTER_PERSISTENCE_MAX_INTERVAL_MS =
        "alluxio.master.persistence.max.interval.ms";
    public static final String MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS =
        "alluxio.master.persistence.scheduler.interval.ms";
    public static final String MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL =
        "alluxio.master.log.config.report.heartbeat.interval";
    public static final String MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR =
        "alluxio.master.periodic.block.integrity.check.repair";
    public static final String MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL =
        "alluxio.master.periodic.block.integrity.check.interval";
    public static final String MASTER_PRINCIPAL = "alluxio.master.principal";
    public static final String MASTER_REPLICATION_CHECK_INTERVAL_MS =
        "alluxio.master.replication.check.interval.ms";
    public static final String MASTER_RETRY = "alluxio.master.retry";
    public static final String MASTER_RPC_PORT = "alluxio.master.port";
    public static final String MASTER_SERVING_THREAD_TIMEOUT =
        "alluxio.master.serving.thread.timeout";
    public static final String MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED =
        "alluxio.master.startup.block.integrity.check.enabled";
    public static final String MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED =
        "alluxio.master.startup.consistency.check.enabled";
    public static final String MASTER_THRIFT_SHUTDOWN_TIMEOUT =
        "alluxio.master.thrift.shutdown.timeout";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
        "alluxio.master.tieredstore.global.level0.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
        "alluxio.master.tieredstore.global.level1.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
        "alluxio.master.tieredstore.global.level2.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVELS =
        "alluxio.master.tieredstore.global.levels";
    public static final String MASTER_TTL_CHECKER_INTERVAL_MS =
        "alluxio.master.ttl.checker.interval";
    public static final String MASTER_UFS_BLOCK_LOCATION_CACHE_CAPACITY =
        "alluxio.master.ufs.block.location.cache.capacity";
    public static final String MASTER_UFS_PATH_CACHE_CAPACITY =
        "alluxio.master.ufs.path.cache.capacity";
    public static final String MASTER_UFS_PATH_CACHE_THREADS =
        "alluxio.master.ufs.path.cache.threads";
    public static final String MASTER_WEB_BIND_HOST = "alluxio.master.web.bind.host";
    public static final String MASTER_WEB_HOSTNAME = "alluxio.master.web.hostname";
    public static final String MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String MASTER_WHITELIST = "alluxio.master.whitelist";
    public static final String MASTER_WORKER_CONNECT_WAIT_TIME =
        "alluxio.master.worker.connect.wait.time";
    public static final String MASTER_WORKER_THREADS_MAX = "alluxio.master.worker.threads.max";
    public static final String MASTER_WORKER_THREADS_MIN = "alluxio.master.worker.threads.min";
    public static final String MASTER_WORKER_TIMEOUT_MS = "alluxio.master.worker.timeout";
    public static final String MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES =
        "alluxio.master.journal.checkpoint.period.entries";
    public static final String MASTER_JOURNAL_GC_PERIOD_MS = "alluxio.master.journal.gc.period";
    public static final String MASTER_JOURNAL_GC_THRESHOLD_MS =
        "alluxio.master.journal.gc.threshold";
    public static final String MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
        "alluxio.master.journal.temporary.file.gc.threshold";

    //
    // Worker related properties
    //
    public static final String WORKER_ALLOCATOR_CLASS = "alluxio.worker.allocator.class";
    public static final String WORKER_BIND_HOST = "alluxio.worker.bind.host";
    public static final String WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.block.heartbeat.interval";
    public static final String WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
        "alluxio.worker.block.heartbeat.timeout";
    public static final String WORKER_BLOCK_THREADS_MAX = "alluxio.worker.block.threads.max";
    public static final String WORKER_BLOCK_THREADS_MIN = "alluxio.worker.block.threads.min";
    public static final String WORKER_DATA_BIND_HOST = "alluxio.worker.data.bind.host";
    public static final String WORKER_DATA_FOLDER = "alluxio.worker.data.folder";
    public static final String WORKER_DATA_FOLDER_PERMISSIONS =
        "alluxio.worker.data.folder.permissions";
    public static final String WORKER_DATA_HOSTNAME = "alluxio.worker.data.hostname";
    public static final String WORKER_DATA_PORT = "alluxio.worker.data.port";
    public static final String WORKER_DATA_SERVER_CLASS = "alluxio.worker.data.server.class";
    public static final String WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS =
        "alluxio.worker.data.server.domain.socket.address";
    public static final String WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID =
        "alluxio.worker.data.server.domain.socket.as.uuid";
    public static final String WORKER_DATA_TMP_FOLDER = "alluxio.worker.data.folder.tmp";
    public static final String WORKER_DATA_TMP_SUBDIR_MAX = "alluxio.worker.data.tmp.subdir.max";
    public static final String WORKER_EVICTOR_CLASS = "alluxio.worker.evictor.class";
    public static final String WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR =
        "alluxio.worker.evictor.lrfu.attenuation.factor";
    public static final String WORKER_EVICTOR_LRFU_STEP_FACTOR =
        "alluxio.worker.evictor.lrfu.step.factor";
    public static final String WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.filesystem.heartbeat.interval";
    public static final String WORKER_FILE_PERSIST_POOL_SIZE =
        "alluxio.worker.file.persist.pool.size";
    public static final String WORKER_FILE_PERSIST_RATE_LIMIT =
        "alluxio.worker.file.persist.rate.limit";
    public static final String WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED =
        "alluxio.worker.file.persist.rate.limit.enabled";
    public static final String WORKER_FILE_BUFFER_SIZE = "alluxio.worker.file.buffer.size";
    public static final String WORKER_FREE_SPACE_TIMEOUT = "alluxio.worker.free.space.timeout";
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
    public static final String WORKER_MASTER_CONNECT_RETRY_TIMEOUT =
        "alluxio.worker.master.connect.retry.timeout";
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
    public static final String WORKER_NETWORK_NETTY_ASYNC_CACHE_MANAGER_THREADS_MAX =
        "alluxio.worker.network.netty.async.cache.manager.threads.max";
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
    public static final String WORKER_SESSION_TIMEOUT_MS = "alluxio.worker.session.timeout";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
        "alluxio.worker.tieredstore.block.lock.readers";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCKS =
        "alluxio.worker.tieredstore.block.locks";
    public static final String WORKER_TIERED_STORE_LEVELS = "alluxio.worker.tieredstore.levels";
    public static final String WORKER_TIERED_STORE_RESERVER_ENABLED =
        "alluxio.worker.tieredstore.reserver.enabled";
    public static final String WORKER_TIERED_STORE_RESERVER_INTERVAL_MS =
        "alluxio.worker.tieredstore.reserver.interval";
    public static final String WORKER_TIERED_STORE_RETRY = "alluxio.worker.tieredstore.retry";
    public static final String WORKER_WEB_BIND_HOST = "alluxio.worker.web.bind.host";
    public static final String WORKER_WEB_HOSTNAME = "alluxio.worker.web.hostname";
    public static final String WORKER_WEB_PORT = "alluxio.worker.web.port";
    public static final String WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS =
        "alluxio.worker.ufs.block.open.timeout";
    public static final String WORKER_UFS_INSTREAM_CACHE_EXPIRATION_TIME =
        "alluxio.worker.ufs.instream.cache.expiration.time";
    public static final String WORKER_UFS_INSTREAM_CACHE_ENABLED =
        "alluxio.worker.ufs.instream.cache.enabled";
    public static final String WORKER_UFS_INSTREAM_CACHE_MAX_SIZE =
        "alluxio.worker.ufs.instream.cache.max.size";

    //
    // Proxy related properties
    //
    public static final String PROXY_S3_WRITE_TYPE = "alluxio.proxy.s3.writetype";
    public static final String PROXY_S3_DELETE_TYPE = "alluxio.proxy.s3.deletetype";
    public static final String PROXY_S3_MULTIPART_TEMPORARY_DIR_SUFFIX =
        "alluxio.proxy.s3.multipart.temporary.dir.suffix";
    public static final String PROXY_STREAM_CACHE_TIMEOUT_MS =
        "alluxio.proxy.stream.cache.timeout";
    public static final String PROXY_WEB_BIND_HOST = "alluxio.proxy.web.bind.host";
    public static final String PROXY_WEB_HOSTNAME = "alluxio.proxy.web.hostname";
    public static final String PROXY_WEB_PORT = "alluxio.proxy.web.port";

    //
    // Locality related properties
    //
    public static final String LOCALITY_ORDER = "alluxio.locality.order";
    public static final String LOCALITY_SCRIPT = "alluxio.locality.script";
    public static final String LOCALITY_COMPARE_NODE_IP = "alluxio.locality.compare.node.ip";

    //
    // Log server related properties
    //
    public static final String LOGSERVER_LOGS_DIR = "alluxio.logserver.logs.dir";
    public static final String LOGSERVER_HOSTNAME = "alluxio.logserver.hostname";
    public static final String LOGSERVER_PORT = "alluxio.logserver.port";
    public static final String LOGSERVER_THREADS_MAX = "alluxio.logserver.threads.max";
    public static final String LOGSERVER_THREADS_MIN = "alluxio.logserver.threads.min";

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
        "alluxio.user.block.worker.client.pool.gc.threshold";
    public static final String USER_BLOCK_WORKER_CLIENT_READ_RETRY =
        "alluxio.user.block.worker.client.read.retry";
    public static final String USER_CONF_CLUSTER_DEFAULT_ENABLED =
        "alluxio.user.conf.cluster.default.enabled";
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
    public static final String USER_FILE_METADATA_SYNC_INTERVAL =
        "alluxio.user.file.metadata.sync.interval";
    public static final String USER_FILE_PASSIVE_CACHE_ENABLED =
        "alluxio.user.file.passive.cache.enabled";
    public static final String USER_FILE_LOAD_TTL =
        "alluxio.user.file.load.ttl";
    public static final String USER_FILE_LOAD_TTL_ACTION =
        "alluxio.user.file.load.ttl.action";
    public static final String USER_FILE_READ_TYPE_DEFAULT = "alluxio.user.file.readtype.default";
    public static final String USER_FILE_REPLICATION_MAX = "alluxio.user.file.replication.max";
    public static final String USER_FILE_REPLICATION_MIN = "alluxio.user.file.replication.min";
    public static final String USER_FILE_REPLICATION_DURABLE =
        "alluxio.user.file.replication.durable";
    public static final String USER_FILE_UFS_TIER_ENABLED = "alluxio.user.file.ufs.tier.enabled";
    public static final String USER_FILE_SEEK_BUFFER_SIZE_BYTES =
        "alluxio.user.file.seek.buffer.size.bytes";
    public static final String USER_FILE_WAITCOMPLETED_POLL_MS =
        "alluxio.user.file.waitcompleted.poll";
    public static final String USER_FILE_CREATE_TTL =
        "alluxio.user.file.create.ttl";
    public static final String USER_FILE_CREATE_TTL_ACTION =
        "alluxio.user.file.create.ttl.action";
    public static final String USER_FILE_WRITE_LOCATION_POLICY =
        "alluxio.user.file.write.location.policy.class";
    public static final String USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES =
        "alluxio.user.file.write.avoid.eviction.policy.reserved.size.bytes";
    public static final String USER_FILE_WRITE_TYPE_DEFAULT = "alluxio.user.file.writetype.default";
    public static final String USER_FILE_WRITE_TIER_DEFAULT =
        "alluxio.user.file.write.tier.default";
    public static final String USER_HEARTBEAT_INTERVAL_MS = "alluxio.user.heartbeat.interval";
    public static final String USER_HOSTNAME = "alluxio.user.hostname";
    public static final String USER_LOCAL_READER_PACKET_SIZE_BYTES =
        "alluxio.user.local.reader.packet.size.bytes";
    public static final String USER_LOCAL_WRITER_PACKET_SIZE_BYTES =
        "alluxio.user.local.writer.packet.size.bytes";
    public static final String USER_METRICS_COLLECTION_ENABLED =
        "alluxio.user.metrics.collection.enabled";
    public static final String USER_METRICS_HEARTBEAT_INTERVAL_MS =
        "alluxio.user.metrics.heartbeat.interval";
    public static final String USER_APP_ID = "alluxio.user.app.id";
    public static final String USER_NETWORK_NETTY_CHANNEL = "alluxio.user.network.netty.channel";
    public static final String USER_NETWORK_NETTY_TIMEOUT_MS =
        "alluxio.user.network.netty.timeout";
    public static final String USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS =
        "alluxio.user.network.netty.writer.close.timeout";
    public static final String USER_NETWORK_NETTY_WRITER_FLUSH_TIMEOUT =
        "alluxio.user.network.netty.writer.flush.timeout";
    public static final String USER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.user.network.netty.worker.threads";
    public static final String USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX =
        "alluxio.user.network.netty.channel.pool.size.max";
    public static final String USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS =
        "alluxio.user.network.netty.channel.pool.gc.threshold";
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
    public static final String USER_NETWORK_SOCKET_TIMEOUT =
        "alluxio.user.network.socket.timeout";
    public static final String USER_RPC_RETRY_BASE_SLEEP_MS =
        "alluxio.user.rpc.retry.base.sleep";
    public static final String USER_RPC_RETRY_MAX_DURATION =
        "alluxio.user.rpc.retry.max.duration";
    public static final String USER_RPC_RETRY_MAX_NUM_RETRY =
        "alluxio.user.rpc.retry.max.num.retry";
    public static final String USER_RPC_RETRY_MAX_SLEEP_MS = "alluxio.user.rpc.retry.max.sleep";
    public static final String USER_UFS_DELEGATION_ENABLED = "alluxio.user.ufs.delegation.enabled";
    public static final String USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.read.buffer.size.bytes";
    public static final String USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.write.buffer.size.bytes";
    public static final String USER_UFS_FILE_READER_CLASS = "alluxio.user.ufs.file.reader.class";
    public static final String USER_UFS_FILE_WRITER_CLASS = "alluxio.user.ufs.file.writer.class";
    public static final String USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED =
        "alluxio.user.ufs.block.location.all.fallback.enabled";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY =
        "alluxio.user.ufs.block.read.location.policy";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
        "alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards";
    public static final String USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
        "alluxio.user.ufs.block.read.concurrency.max";
    public static final String USER_SHORT_CIRCUIT_ENABLED = "alluxio.user.short.circuit.enabled";
    public static final String USER_WORKER_LIST_REFRESH_INTERVAL =
        "alluxio.user.worker.list.refresh.interval";

    //
    // FUSE integration related properties
    //
    public static final String FUSE_CACHED_PATHS_MAX = "alluxio.fuse.cached.paths.max";
    public static final String FUSE_DEBUG_ENABLED = "alluxio.fuse.debug.enabled";
    public static final String FUSE_FS_NAME = "alluxio.fuse.fs.name";
    public static final String FUSE_MAXWRITE_BYTES = "alluxio.fuse.maxwrite.bytes";

    //
    // Security related properties
    //
    public static final String SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS =
        "alluxio.security.authentication.custom.provider.class";
    public static final String SECURITY_AUTHENTICATION_TYPE =
        "alluxio.security.authentication.type";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
        "alluxio.security.authorization.permission.enabled";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
        "alluxio.security.authorization.permission.supergroup";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_UMASK =
        "alluxio.security.authorization.permission.umask";
    public static final String SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
        "alluxio.security.group.mapping.cache.timeout";
    public static final String SECURITY_GROUP_MAPPING_CLASS =
        "alluxio.security.group.mapping.class";
    public static final String SECURITY_LOGIN_IMPERSONATION_USERNAME =
        "alluxio.security.login.impersonation.username";
    public static final String SECURITY_LOGIN_USERNAME = "alluxio.security.login.username";

    //
    // Job service
    //
    public static final String JOB_MASTER_CLIENT_THREADS =
        "alluxio.job.master.client.threads";
    public static final String JOB_MASTER_FINISHED_JOB_RETENTION_MS =
        "alluxio.job.master.finished.job.retention.ms";
    public static final String JOB_MASTER_JOB_CAPACITY = "alluxio.job.master.job.capacity";
    public static final String JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS =
        "alluxio.job.master.worker.heartbeat.interval.ms";
    public static final String JOB_MASTER_WORKER_TIMEOUT_MS =
        "alluxio.job.master.worker.timeout.ms";

    public static final String JOB_MASTER_BIND_HOST = "alluxio.job.master.bind.host";
    public static final String JOB_MASTER_HOSTNAME = "alluxio.job.master.hostname";
    public static final String JOB_MASTER_LOST_WORKER_INTERVAL_MS =
        "alluxio.job.master.lost.worker.interval.ms";
    public static final String JOB_MASTER_RPC_PORT = "alluxio.job.master.rpc.port";
    public static final String JOB_MASTER_WEB_BIND_HOST = "alluxio.job.master.web.bind.host";
    public static final String JOB_MASTER_WEB_HOSTNAME = "alluxio.job.master.web.hostname";
    public static final String JOB_MASTER_WEB_PORT = "alluxio.job.master.web.port";

    public static final String JOB_WORKER_BIND_HOST = "alluxio.job.worker.bind.host";
    public static final String JOB_WORKER_DATA_PORT = "alluxio.job.worker.data.port";
    public static final String JOB_WORKER_RPC_PORT = "alluxio.job.worker.rpc.port";
    public static final String JOB_WORKER_WEB_BIND_HOST = "alluxio.job.worker.web.bind.host";
    public static final String JOB_WORKER_WEB_PORT = "alluxio.job.worker.web.port";

    public static final String ZOOKEEPER_JOB_ELECTION_PATH = "alluxio.zookeeper.job.election.path";
    public static final String ZOOKEEPER_JOB_LEADER_PATH = "alluxio.zookeeper.job.leader.path";

    //
    // JVM Monitor related properties
    //
    public static final String JVM_MONITOR_WARN_THRESHOLD_MS =
        "alluxio.jvm.monitor.warn.threshold";
    public static final String JVM_MONITOR_INFO_THRESHOLD_MS =
        "alluxio.jvm.monitor.info.threshold";
    public static final String JVM_MONITOR_SLEEP_INTERVAL_MS =
        "alluxio.jvm.monitor.sleep.interval";
    public static final String MASTER_JVM_MONITOR_ENABLED = "alluxio.master.jvm.monitor.enabled";
    public static final String WORKER_JVM_MONITOR_ENABLED = "alluxio.worker.jvm.monitor.enabled";

    private Name() {} // prevent instantiation
  }

  /**
   * A set of templates to generate the names of parameterized properties given
   * different parameters. E.g., * {@code Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
   */
  @ThreadSafe
  public enum Template {
    LOCALITY_TIER("alluxio.locality.%s", "alluxio\\.locality\\.(\\w+)"),
    MASTER_IMPERSONATION_GROUPS_OPTION("alluxio.master.security.impersonation.%s.groups",
        "alluxio\\.master\\.security\\.impersonation\\.([a-zA-Z_0-9-\\.@]+)\\.groups"),
    MASTER_IMPERSONATION_USERS_OPTION("alluxio.master.security.impersonation.%s.users",
        "alluxio\\.master\\.security\\.impersonation\\.([a-zA-Z_0-9-\\.@]+)\\.users"),
    MASTER_JOURNAL_UFS_OPTION("alluxio.master.journal.ufs.option",
        "alluxio\\.master\\.journal\\.ufs\\.option"),
    MASTER_JOURNAL_UFS_OPTION_PROPERTY("alluxio.master.journal.ufs.option.%s",
        "alluxio\\.master\\.journal\\.ufs\\.option\\.(?<nested>(\\w+\\.)*+\\w+)",
        PropertyCreators.NESTED_JOURNAL_PROPERTY_CREATOR),
    MASTER_MOUNT_TABLE_ALLUXIO("alluxio.master.mount.table.%s.alluxio",
        "alluxio\\.master\\.mount\\.table.(\\w+)\\.alluxio"),
    MASTER_MOUNT_TABLE_OPTION("alluxio.master.mount.table.%s.option",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option"),
    MASTER_MOUNT_TABLE_OPTION_PROPERTY("alluxio.master.mount.table.%s.option.%s",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option\\.(?<nested>(\\w+\\.)*+\\w+)",
        PropertyCreators.NESTED_UFS_PROPERTY_CREATOR),
    MASTER_MOUNT_TABLE_READONLY("alluxio.master.mount.table.%s.readonly",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.readonly"),
    MASTER_MOUNT_TABLE_SHARED("alluxio.master.mount.table.%s.shared",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.shared"),
    MASTER_MOUNT_TABLE_UFS("alluxio.master.mount.table.%s.ufs",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.ufs"),
    MASTER_MOUNT_TABLE_ROOT_OPTION_PROPERTY("alluxio.master.mount.table.root.option.%s",
        "alluxio\\.master\\.mount\\.table\\.root\\.option\\.(?<nested>(\\w+\\.)*+\\w+)",
        PropertyCreators.NESTED_UFS_PROPERTY_CREATOR),
    MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS("alluxio.master.tieredstore.global.level%d.alias",
        "alluxio\\.master\\.tieredstore\\.global\\.level(\\d+)\\.alias"),
    UNDERFS_AZURE_ACCOUNT_KEY(
        "fs.azure.account.key.%s.blob.core.windows.net",
        "fs\\.azure\\.account\\.key\\.(\\w+)\\.blob\\.core\\.windows\\.net",
        PropertyCreators.fromBuilder(new Builder("fs.azure.account.key.%s.blob.core.windows.net")
            .setDisplayType(DisplayType.CREDENTIALS))),
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

    // puts property creators in a nested class to avoid NPE in enum static initialization
    private static class PropertyCreators {
      private static final BiFunction<String, PropertyKey, PropertyKey> DEFAULT_PROPERTY_CREATOR =
          fromBuilder(new Builder(""));
      private static final BiFunction<String, PropertyKey, PropertyKey>
          NESTED_UFS_PROPERTY_CREATOR =
          createNestedPropertyCreator(Scope.SERVER, ConsistencyCheckLevel.ENFORCE);
      private static final BiFunction<String, PropertyKey, PropertyKey>
          NESTED_JOURNAL_PROPERTY_CREATOR =
          createNestedPropertyCreator(Scope.MASTER, ConsistencyCheckLevel.ENFORCE);

      private static BiFunction<String, PropertyKey, PropertyKey> fromBuilder(Builder builder) {
        return (name, baseProperty) -> builder.setName(name).buildUnregistered();
      }

      private static BiFunction<String, PropertyKey, PropertyKey> createNestedPropertyCreator(
          Scope scope, ConsistencyCheckLevel consistencyCheckLevel) {
        return (name, baseProperty) -> {
          Builder builder = new Builder(name)
              .setScope(scope)
              .setConsistencyCheckLevel(consistencyCheckLevel);
          if (baseProperty != null) {
            builder.setDisplayType(baseProperty.getDisplayType());
            builder.setDefaultSupplier(baseProperty.getDefaultSupplier());
          }
          return builder.buildUnregistered();
        };
      }
    }

    private static final String NESTED_GROUP = "nested";
    private final String mFormat;
    private final Pattern mPattern;
    private BiFunction<String, PropertyKey, PropertyKey> mPropertyCreator =
        PropertyCreators.DEFAULT_PROPERTY_CREATOR;

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

    /**
     * Constructs a nested property key format with a function to construct property key given
     * base property key.
     *
     * @param format String of this property as formatted string
     * @param re String of this property as regexp
     * @param propertyCreator a function that creates property key given name and base property key
     *                        (for nested properties only, will be null otherwise)
     */
    Template(String format, String re,
        BiFunction<String, PropertyKey, PropertyKey> propertyCreator) {
      this(format, re);
      mPropertyCreator = propertyCreator;
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

    /**
     * @param input the input property key string
     * @return whether the input string matches this template
     */
    public boolean matches(String input) {
      Matcher matcher = mPattern.matcher(input);
      return matcher.matches();
    }

    /**
     * @param input the input property key string
     * @return the matcher matching the template to the string
     */
    public Matcher match(String input) {
      return mPattern.matcher(input);
    }

    /**
     * Gets the property key if the property name matches the template.
     *
     * @param propertyName name of the property
     * @return the property key, or null if the property name does not match the template
     */
    @Nullable
    private PropertyKey getPropertyKey(String propertyName) {
      Matcher matcher = match(propertyName);
      if (!matcher.matches()) {
        return null;
      }
      // if the template can extract a nested property, build the new property from the nested one
      String nestedKeyName = null;
      try {
        nestedKeyName = matcher.group(NESTED_GROUP);
      } catch (IllegalArgumentException e) {
        // ignore if group is not found
      }
      PropertyKey nestedProperty = null;
      if (nestedKeyName != null && isValid(nestedKeyName)) {
        nestedProperty = fromString(nestedKeyName);
      }
      return mPropertyCreator.apply(propertyName, nestedProperty);
    }
  }

  /**
   * @param input string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String input) {
    // Check if input matches any default keys or aliases
    if (DEFAULT_KEYS_MAP.containsKey(input) || DEFAULT_ALIAS_MAP.containsKey(input)) {
      return true;
    }
    // Check if input matches any parameterized keys
    for (Template template : Template.values()) {
      if (template.matches(input)) {
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
    // Try to match input with alias
    key = DEFAULT_ALIAS_MAP.get(input);
    if (key != null) {
      return key;
    }
    // Try different templates and see if any template matches
    for (Template template : Template.values()) {
      key = template.getPropertyKey(input);
      if (key != null) {
        return key;
      }
    }
    throw new IllegalArgumentException(
        ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(input));
  }

  /**
   * @return all pre-defined property keys
   */
  public static Collection<? extends PropertyKey> defaultKeys() {
    return Sets.newHashSet(DEFAULT_KEYS_MAP.values());
  }

  /** Property name. */
  private final String mName;

  /** Property Key description. */
  private final String mDescription;

  /** Supplies the Property Key default value. */
  private final DefaultSupplier mDefaultSupplier;

  /** Property Key alias. */
  private final String[] mAliases;

  /** Whether to ignore as a site property. */
  private final boolean mIgnoredSiteProperty;

  /** Whether the property is an Alluxio built-in property. */
  private final boolean mIsBuiltIn;

  /** Whether to hide in document. */
  private final boolean mIsHidden;

  /** Whether property should be consistent within the cluster. */
  private final ConsistencyCheckLevel mConsistencyCheckLevel;

  /** The scope this property applies to. */
  private final Scope mScope;

  /** The displayType which indicates how the property value should be displayed. **/
  private final DisplayType mDisplayType;

  /**
   * @param name String of this property
   * @param description String description of this property key
   * @param defaultSupplier default value supplier
   * @param aliases alias of this property key
   * @param ignoredSiteProperty true if Alluxio ignores user-specified value for this property in
   *        site properties file
   * @param isHidden whether to hide in document
   * @param consistencyCheckLevel the consistency check level to apply to this property
   * @param scope the scope this property applies to
   * @param displayType how the property value should be displayed
   * @param isBuiltIn whether this is an Alluxio built-in property
   */
  private PropertyKey(String name, String description, DefaultSupplier defaultSupplier,
      String[] aliases, boolean ignoredSiteProperty, boolean isHidden,
      ConsistencyCheckLevel consistencyCheckLevel, Scope scope, DisplayType displayType,
      boolean isBuiltIn) {
    mName = Preconditions.checkNotNull(name, "name");
    // TODO(binfan): null check after we add description for each property key
    mDescription = Strings.isNullOrEmpty(description) ? "N/A" : description;
    mDefaultSupplier = defaultSupplier;
    mAliases = aliases;
    mIgnoredSiteProperty = ignoredSiteProperty;
    mIsHidden = isHidden;
    mConsistencyCheckLevel = consistencyCheckLevel;
    mScope = scope;
    mDisplayType = displayType;
    mIsBuiltIn = isBuiltIn;
  }

  /**
   * @param name String of this property
   */
  private PropertyKey(String name) {
    this(name, null, new DefaultSupplier(() -> null, "null"), null, false, false,
        ConsistencyCheckLevel.IGNORE, Scope.ALL, DisplayType.DEFAULT, true);
  }

  /**
   * Registers the given key to the global key map.
   *
   * @param key th property
   * @return whether the property key is successfully registered
   */
  @VisibleForTesting
  public static boolean register(PropertyKey key) {
    String name = key.getName();
    String[] aliases = key.getAliases();
    if (DEFAULT_KEYS_MAP.containsKey(name)) {
      if (DEFAULT_KEYS_MAP.get(name).isBuiltIn() || !key.isBuiltIn()) {
        return false;
      }
    }
    DEFAULT_KEYS_MAP.put(name, key);
    if (aliases != null) {
      for (String alias : aliases) {
        DEFAULT_ALIAS_MAP.put(alias, key);
      }
    }
    return true;
  }

  /**
   * Unregisters the given key from the global key map.
   *
   * @param key the property to unregister
   */
  @VisibleForTesting
  public static void unregister(PropertyKey key) {
    String name = key.getName();
    DEFAULT_KEYS_MAP.remove(name);
    DEFAULT_ALIAS_MAP.remove(name);
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

  @Override
  public int compareTo(PropertyKey o) {
    return mName.compareTo(o.mName);
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
   * @return the name of the property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the alias of a property
   */
  public String[] getAliases() {
    return mAliases;
  }

  /**
   * @return the description of a property
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the default value of a property key or null if value not set
   */
  @Nullable
  public String getDefaultValue() {
    Object defaultValue = mDefaultSupplier.get();
    return defaultValue == null ? null : defaultValue.toString();
  }

  /**
   * @return the default supplier of a property key
   */
  public DefaultSupplier getDefaultSupplier() {
    return mDefaultSupplier;
  }

  /**
   * @return true if this property should be ignored as a site property
   */
  public boolean isIgnoredSiteProperty() {
    return mIgnoredSiteProperty;
  }

  /**
   * @return true if this property is built-in
   */
  public boolean isBuiltIn() {
    return mIsBuiltIn;
  }

  /**
   * @return true if this property should not show up in the document
   */
  public boolean isHidden() {
    return mIsHidden;
  }

  /**
   * @return the consistency check level to apply to this property
   */
  public ConsistencyCheckLevel getConsistencyLevel() {
    return mConsistencyCheckLevel;
  }

  /**
   * @return the scope which this property applies to
   */
  public Scope getScope() {
    return mScope;
  }

  /**
   * @return the displayType which indicates how the property value should be displayed
   */
  public DisplayType getDisplayType() {
    return mDisplayType;
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
