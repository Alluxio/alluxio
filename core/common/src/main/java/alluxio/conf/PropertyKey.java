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

package alluxio.conf;

import alluxio.Constants;
import alluxio.DefaultSupplier;
import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.annotation.PublicApi;
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.Scope;
import alluxio.grpc.WritePType;
import alluxio.util.OSUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
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
@PublicApi
public final class PropertyKey implements Comparable<PropertyKey> {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyKey.class);

  // The following two maps must be the first to initialize within this file.
  /** A map from default property key's string name to the key. */
  private static final Map<String, PropertyKey> DEFAULT_KEYS_MAP = new ConcurrentHashMap<>();
  /** A map from default property key's alias to the key. */
  private static final Map<String, PropertyKey> DEFAULT_ALIAS_MAP = new ConcurrentHashMap<>();
  /** A cache storing result for template regexp matching results. */
  private static final Cache<String, Boolean> REGEXP_CACHE = CacheBuilder.newBuilder()
      .maximumSize(1024)
      .build();
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
     * @param aliases aliases for the property
     * @return the updated builder instance
     */
    public Builder setAlias(String... aliases) {
      mAlias = Arrays.copyOf(aliases, aliases.length);
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
      return MoreObjects.toStringHelper(this)
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey CONF_VALIDATION_ENABLED =
      new Builder(Name.CONF_VALIDATION_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to validate the configuration properties when initializing "
              + "Alluxio clients or server process.")
          .setIsHidden(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey DEBUG =
      new Builder(Name.DEBUG)
          .setDefaultValue(false)
          .setDescription("Set to true to enable debug mode which has additional logging and "
              + "info in the Web UI.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey LOGGER_TYPE =
      new Builder(Name.LOGGER_TYPE)
          .setDefaultValue("Console")
          .setDescription("The type of logger.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey LOGS_DIR =
      new Builder(Name.LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", Name.WORK_DIR))
          .setDescription("The path under Alluxio home directory to store log files. It has a "
              + "corresponding environment variable $ALLUXIO_LOGS_DIR.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  // Used in alluxio-config.sh and conf/log4j.properties
  public static final PropertyKey USER_LOGS_DIR =
      new Builder(Name.USER_LOGS_DIR)
          .setDefaultValue(String.format("${%s}/user", Name.LOGS_DIR))
          .setDescription("The path to store logs of Alluxio shell. To change its value, one can "
              + " set environment variable $ALLUXIO_USER_LOGS_DIR.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey METRICS_CONF_FILE =
      new Builder(Name.METRICS_CONF_FILE)
          .setDefaultValue(String.format("${%s}/metrics.properties", Name.CONF_DIR))
          .setDescription("The file path of the metrics system configuration file. By default "
              + "it is `metrics.properties` in the `conf` directory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
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
  public static final PropertyKey NETWORK_CONNECTION_AUTH_TIMEOUT =
      new Builder(Name.NETWORK_CONNECTION_AUTH_TIMEOUT)
          .setDefaultValue("30sec")
          .setDescription("Maximum time to wait for a connection (gRPC channel) to attempt to "
              + "receive an authentication response.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT =
      new Builder(Name.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT)
          .setAlias("alluxio.network.connection.health.check.timeout.ms")
          .setDefaultValue("5sec")
          .setDescription("Allowed duration for checking health of client connections (gRPC "
              + "channels) before being assigned to a client. If a connection does not become "
              + "active  within configured time, it will be shut down and a new connection will be "
              + "created for the client")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT =
      new Builder(Name.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT)
          .setDefaultValue("60sec")
          .setDescription("Maximum time to wait for gRPC server to stop on shutdown")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT =
      new Builder(Name.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT)
          .setDefaultValue("45sec")
          .setDescription("Maximum time to wait for connections (gRPC channels) to stop on "
              + "shutdown")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey NETWORK_CONNECTION_SHUTDOWN_TIMEOUT =
      new Builder(Name.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT)
          .setDefaultValue("15sec")
          .setDescription("Maximum time to wait for connections (gRPC channels) to stop after "
              + "graceful shutdown attempt.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
      new Builder(Name.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)
          .setAlias("alluxio.network.host.resolution.timeout.ms")
          .setDefaultValue("5sec")
          .setDescription("During startup of the Master and Worker processes Alluxio needs to "
              + "ensure that they are listening on externally resolvable and reachable host "
              + "names. To do this, Alluxio will automatically attempt to select an "
              + "appropriate host name if one was not explicitly specified. This represents "
              + "the maximum amount of time spent waiting to determine if a candidate host "
              + "name is resolvable over the network.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey TEST_MODE =
      new Builder(Name.TEST_MODE)
          .setDefaultValue(false)
          .setDescription("Flag used only during tests to allow special behavior.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setIsHidden(true)
          .setScope(Scope.ALL)
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
          .setScope(Scope.ALL)
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
          .setDefaultValue(String.format("${%s}/webui/", Name.HOME))
          .setDescription("Path to the web UI resources. User should never modify this property.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .setIsHidden(true)
          .build();
  public static final PropertyKey WEB_THREADS =
      new Builder(Name.WEB_THREADS)
          .setDefaultValue(1)
          .setDescription("How many threads to serve Alluxio web UI.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_CORS_ENABLED =
      new Builder(Name.WEB_CORS_ENABLED)
          .setDefaultValue(false)
          .setDescription("Set to true to enable Cross-Origin Resource Sharing for RESTful API"
              + "endpoints.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_REFRESH_INTERVAL =
      new Builder(Name.WEB_REFRESH_INTERVAL)
          .setDefaultValue("15s")
          .setDescription("The amount of time to await before refreshing the Web UI if it is set "
              + "to auto refresh.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WEB_UI_ENABLED =
      new Builder(Name.WEB_UI_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether the master/worker will have Web UI enabled. "
              + "If set to false, the master/worker will not have Web UI page, but the RESTful "
              + "endpoints and metrics will still be available.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey WORK_DIR =
      new Builder(Name.WORK_DIR)
          .setDefaultValue(String.format("${%s}", Name.HOME))
          .setDescription("The directory to use for Alluxio's working directory. By default, "
              + "the journal, logs, and under file storage data (if using local filesystem) "
              + "are written here.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey ZOOKEEPER_ADDRESS =
      new Builder(Name.ZOOKEEPER_ADDRESS)
          .setDescription("Address of ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_CONNECTION_TIMEOUT =
      new Builder(Name.ZOOKEEPER_CONNECTION_TIMEOUT)
          .setDefaultValue("15s") // matches Zookeeper's default
          .setDescription("Connection timeout for Alluxio (job) masters to select "
              + "the leading (job) master when connecting to Zookeeper")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey ZOOKEEPER_ELECTION_PATH =
      new Builder(Name.ZOOKEEPER_ELECTION_PATH)
          .setDefaultValue("/alluxio/election")
          .setDescription("Election directory in ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_ENABLED =
      new Builder(Name.ZOOKEEPER_ENABLED)
          .setDefaultValue(false)
          .setDescription("If true, setup master fault tolerant mode using ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT =
      new Builder(Name.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT)
          .setDefaultValue(10)
          .setDescription("The number of retries to inquire leader from ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_PATH =
      new Builder(Name.ZOOKEEPER_LEADER_PATH)
          .setDefaultValue("/alluxio/leader")
          .setDescription("Leader directory in ZooKeeper.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_SESSION_TIMEOUT =
      new Builder(Name.ZOOKEEPER_SESSION_TIMEOUT)
          .setDefaultValue("60s") // matches Zookeeper's default
          .setDescription("Session timeout to use when connecting to Zookeeper")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey ZOOKEEPER_AUTH_ENABLED =
      new Builder(Name.ZOOKEEPER_AUTH_ENABLED)
          .setDefaultValue(true)
          .setDescription("If true, enable client-side Zookeeper authentication.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY =
      new Builder(Name.ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY)
          .setDefaultValue("SESSION")
          .setDescription("Connection error policy defines how errors on zookeeper connections "
              + "to be treated in leader election. "
              + "STANDARD policy treats every connection event as failure."
              + "SESSION policy relies on zookeeper sessions for judging failures, "
              + "helping leader to retain its status, as long as its session is protected.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  /**
   * UFS related properties.
   */
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
          .setDescription("Whether or not to clean up under file storage periodically."
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
              + " mounted under file storages.")
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
  public static final PropertyKey UNDERFS_GCS_DEFAULT_MODE =
      new Builder(Name.UNDERFS_GCS_DEFAULT_MODE)
          .setDefaultValue("0700")
          .setDescription("Mode (in octal notation) for GCS objects if mode cannot be discovered.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_GCS_DIRECTORY_SUFFIX =
      new Builder(Name.UNDERFS_GCS_DIRECTORY_SUFFIX)
          .setDefaultValue("/")
          .setDescription("Directories are represented in GCS as zero-byte objects named with "
              + "the specified suffix.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
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
          .setDescription("Location of the HDFS configuration file to overwrite "
              + "the default HDFS client configuration. Note that, these files must be available"
              + "on every node.")
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
          .setDefaultValue("hdfs://,glusterfs:///")
          .setDescription("Optionally, specify which prefixes should run through the HDFS "
              + "implementation of UnderFileSystem. The delimiter is any whitespace "
              + "and/or ','.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_HDFS_REMOTE =
      new Builder(Name.UNDERFS_HDFS_REMOTE)
          .setDefaultValue(true)
          .setDescription("Boolean indicating whether or not the under storage worker nodes "
              + "are remote with respect to Alluxio worker nodes. If set to true, Alluxio "
              + "will not attempt to discover locality information from the under storage "
              + "because locality is impossible. This will improve performance. The default "
              + "value is true.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_WEB_HEADER_LAST_MODIFIED =
      new Builder(Name.UNDERFS_WEB_HEADER_LAST_MODIFIED)
          .setDefaultValue("EEE, dd MMM yyyy HH:mm:ss zzz")
          .setDescription("Date format of last modified for a http response header.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_WEB_CONNECTION_TIMEOUT =
      new Builder(Name.UNDERFS_WEB_CONNECTION_TIMEOUT)
          .setDefaultValue("60s")
          .setDescription("Default timeout for a http connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_WEB_PARENT_NAMES =
      new Builder(Name.UNDERFS_WEB_PARENT_NAMES)
          .setDefaultValue("Parent Directory,..,../")
          .setDescription("The text of the http link for the parent directory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_WEB_TITLES =
      new Builder(Name.UNDERFS_WEB_TITLES)
          .setDefaultValue("Index of,Directory listing for")
          .setDescription("The title of the content for a http url.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_BREADCRUMBS_ENABLED =
      new Builder(Name.UNDERFS_OBJECT_STORE_BREADCRUMBS_ENABLED)
          .setDefaultValue(true)
          .setDescription("Set this to false to prevent Alluxio from creating zero byte objects "
              + "during read or list operations on object store UFS. Leaving this on enables more"
              + " efficient listing of prefixes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE =
      new Builder(Name.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE)
          .setDefaultValue(String.format("${%s}", Name.USER_BLOCK_SIZE_BYTES_DEFAULT))
          .setDescription("Default chunk size for ranged reads from multi-range object input "
              + "streams.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_OBJECT_STORE_SERVICE_THREADS =
      new Builder(Name.UNDERFS_OBJECT_STORE_SERVICE_THREADS)
          .setDefaultValue(20)
          .setDescription("The number of threads in executor pool for parallel object store "
              + "UFS operations, such as directory renames and deletes.")
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
  public static final PropertyKey UNDERFS_EVENTUAL_CONSISTENCY_RETRY_BASE_SLEEP_MS =
      new Builder(Name.UNDERFS_EVENTUAL_CONSISTENCY_RETRY_BASE_SLEEP_MS)
          .setDefaultValue("50ms")
          .setDescription("To handle eventually consistent storage semantics "
              + "for certain under storages, Alluxio will perform retries "
              + "when under storage metadata doesn't match Alluxio's expectations. "
              + "These retries use exponential backoff. "
              + "This property determines the base time for the exponential backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_NUM =
      new Builder(Name.UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_NUM)
          .setDefaultValue(20)
          .setDescription("To handle eventually consistent storage semantics "
              + "for certain under storages, Alluxio will perform retries "
              + "when under storage metadata doesn't match Alluxio's expectations. "
              + "These retries use exponential backoff. "
              + "This property determines the maximum number of retries.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_SLEEP_MS =
      new Builder(Name.UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_SLEEP_MS)
          .setDefaultValue("30sec")
          .setDescription("To handle eventually consistent storage semantics "
              + "for certain under storages, Alluxio will perform retries "
              + "when under storage metadata doesn't match Alluxio's expectations. "
              + "These retries use exponential backoff. "
              + "This property determines the maximum wait time in the backoff.")
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
          .setAlias("alluxio.underfs.oss.connection.timeout.ms")
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
          .setAlias("alluxio.underfs.oss.socket.timeout.ms")
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
              + "the \"Account Identifiers\" tab and refer to \"Canonical User ID\". "
              + "Unspecified owner id will map to a default empty username")
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
          .setDescription("For an Alluxio worker, this is the maximum number of threads to use "
              + "for uploading data to S3 for multipart uploads. These operations can be fairly "
              + "expensive, so multiple threads are encouraged. However, this also splits the "
              + "bandwidth between threads, meaning the overall latency for completing an upload "
              + "will be higher for more threads. For the Alluxio master, this is the maximum "
              + "number of threads used for the rename (copy) operation. It is recommended that "
              + "value should be greater than or equal to "
              + Name.UNDERFS_OBJECT_STORE_SERVICE_THREADS)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_DEFAULT_MODE =
      new Builder(Name.UNDERFS_S3_DEFAULT_MODE)
          .setAlias("alluxio.underfs.s3a.default.mode")
          .setDefaultValue("0700")
          .setDescription("Mode (in octal notation) for S3 objects if mode cannot be discovered.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_DIRECTORY_SUFFIX =
      new Builder(Name.UNDERFS_S3_DIRECTORY_SUFFIX)
          .setAlias("alluxio.underfs.s3a.directory.suffix")
          .setDefaultValue("/")
          .setDescription("Directories are represented in S3 as zero-byte objects named with "
              + "the specified suffix.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_BULK_DELETE_ENABLED =
      new Builder(Name.UNDERFS_S3_BULK_DELETE_ENABLED)
          .setAlias("alluxio.underfs.s3a.bulk.delete.enabled")
          .setDefaultValue(true)
          .setIsHidden(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_INHERIT_ACL =
      new Builder(Name.UNDERFS_S3_INHERIT_ACL)
          .setAlias("alluxio.underfs.s3a.inherit_acl")
          .setDefaultValue(true)
          .setDescription("Set this property to false to disable inheriting bucket ACLs on "
              + "objects. Note that the translation from bucket ACLs to Alluxio user permissions "
              + "is best effort as some S3-like storage services doe not implement ACLs fully "
              + "compatible with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE =
      new Builder(Name.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE)
          .setAlias("alluxio.underfs.s3a.intermediate.upload.clean.age")
          .setDefaultValue("3day")
          .setDescription("Streaming uploads may not have been completed/aborted correctly "
              + "and need periodical ufs cleanup. If ufs cleanup is enabled, "
              + "intermediate multipart uploads in all non-readonly S3 mount points "
              + "older than this age will be cleaned. This may impact other "
              + "ongoing upload operations, so a large clean age is encouraged.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_LIST_OBJECTS_V1 =
      new Builder(Name.UNDERFS_S3_LIST_OBJECTS_V1)
          .setAlias("alluxio.underfs.s3a.list.objects.v1")
          .setDefaultValue(false)
          .setDescription("Whether to use version 1 of GET Bucket (List Objects) API.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_MAX_ERROR_RETRY =
      new Builder(Name.UNDERFS_S3_MAX_ERROR_RETRY)
          .setAlias("alluxio.underfs.s3a.max.error.retry")
          .setDescription("The maximum number of retry attempts for failed retryable requests."
              + "Setting this property will override the AWS SDK default.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_REQUEST_TIMEOUT =
      new Builder(Name.UNDERFS_S3_REQUEST_TIMEOUT)
          .setAlias("alluxio.underfs.s3a.request.timeout.ms", "alluxio.underfs.s3a.request.timeout")
          .setDefaultValue("1min")
          .setDescription("The timeout for a single request to S3. Infinity if set to 0. "
              + "Setting this property to a non-zero value can improve performance by "
              + "avoiding the long tail of requests to S3. For very slow connections to S3, "
              + "consider increasing this value or setting it to 0.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_SECURE_HTTP_ENABLED =
      new Builder(Name.UNDERFS_S3_SECURE_HTTP_ENABLED)
          .setAlias("alluxio.underfs.s3a.secure.http.enabled")
          .setDefaultValue(false)
          .setDescription("Whether or not to use HTTPS protocol when communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED =
      new Builder(Name.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED)
          .setAlias("alluxio.underfs.s3a.server.side.encryption.enabled")
          .setDefaultValue(false)
          .setDescription("Whether or not to encrypt data stored in S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_SIGNER_ALGORITHM =
      new Builder(Name.UNDERFS_S3_SIGNER_ALGORITHM)
          .setAlias("alluxio.underfs.s3a.signer.algorithm")
          .setDescription("The signature algorithm which should be used to sign requests to "
              + "the s3 service. This is optional, and if not set, the client will "
              + "automatically determine it. For interacting with an S3 endpoint which only "
              + "supports v2 signatures, set this to \"S3SignerType\".")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_SOCKET_TIMEOUT =
      new Builder(Name.UNDERFS_S3_SOCKET_TIMEOUT)
          .setAlias("alluxio.underfs.s3a.socket.timeout.ms", "alluxio.underfs.s3a.socket.timeout")
          .setDefaultValue("50sec")
          .setDescription("Length of the socket timeout when communicating with S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_STREAMING_UPLOAD_ENABLED =
      new Builder(Name.UNDERFS_S3_STREAMING_UPLOAD_ENABLED)
          .setAlias("alluxio.underfs.s3a.streaming.upload.enabled")
          .setDefaultValue(false)
          .setDescription("(Experimental) If true, using streaming upload to write to S3.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE =
      new Builder(Name.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE)
          .setAlias("alluxio.underfs.s3a.streaming.upload.partition.size")
          .setDefaultValue("64MB")
          .setDescription("Maximum allowable size of a single buffer file when using "
              + "S3A streaming upload. When the buffer file reaches the partition size, "
              + "it will be uploaded and the upcoming data will write to other buffer files."
              + "If the partition size is too small, S3A upload speed might be affected. ")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_KODO_REQUESTS_MAX =
      new Builder(Name.UNDERFS_KODO_REQUESTS_MAX)
          .setDefaultValue(64)
          .setDescription("The maximum number of kodo connections.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey UNDERFS_KODO_CONNECT_TIMEOUT =
      new Builder(Name.UNDERFS_KODO_CONNECT_TIMEOUT)
          .setDefaultValue("50sec")
          .setDescription("The connect timeout of kodo.")
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
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey SWIFT_SIMULATION = new Builder(Name.SWIFT_SIMULATION)
      .setDescription("Whether to simulate a single node Swift backend for testing purposes: "
          + "true or false (default).")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .build();
  public static final PropertyKey SWIFT_TENANT_KEY = new Builder(Name.SWIFT_TENANT_KEY)
      .setDescription("Swift user for authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setDisplayType(DisplayType.CREDENTIALS)
      .build();
  public static final PropertyKey SWIFT_USER_KEY = new Builder(Name.SWIFT_USER_KEY)
      .setDescription("Swift tenant for authentication.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setDisplayType(DisplayType.CREDENTIALS)
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
          .setDisplayType(DisplayType.CREDENTIALS)
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
          .setScope(Scope.SERVER)
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
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  // Journal ufs related properties
  public static final PropertyKey MASTER_JOURNAL_UFS_OPTION =
      new Builder(Template.MASTER_JOURNAL_UFS_OPTION)
          .setDescription("The configuration to use for the journal operations.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey KODO_ACCESS_KEY =
      new Builder(Name.KODO_ACCESS_KEY)
          .setDescription("The access key of Kodo bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey KODO_SECRET_KEY =
      new Builder(Name.KODO_SECRET_KEY)
          .setDescription("The secret key of Kodo bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey KODO_DOWNLOAD_HOST =
      new Builder(Name.KODO_DOWNLOAD_HOST)
          .setDescription("The download domain of Kodo bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey KODO_ENDPOINT =
      new Builder(Name.KODO_ENDPOINT)
          .setDescription("The endpoint of Kodo bucket.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.SERVER)
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
          .setAlias("alluxio.underfs.address")
          .setDescription("The storage address of the UFS at the Alluxio root mount point.")
          .setDefaultValue(String.format("${%s}/underFSStorage", Name.WORK_DIR))
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
          .setDescription("Default directory for writing master metadata backups. This path is "
              + "an absolute path of the root UFS. For example, if the root ufs "
              + "directory is hdfs://host:port/alluxio/data, the default backup directory will be "
              + "hdfs://host:port/alluxio_backups.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_ENTRY_BUFFER_COUNT =
      new Builder(Name.MASTER_BACKUP_ENTRY_BUFFER_COUNT)
          .setDefaultValue("10000")
          .setDescription("How many journal entries to buffer during a back-up.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_DELEGATION_ENABLED =
      new Builder(Name.MASTER_BACKUP_DELEGATION_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to delegate journals to stand-by masters in HA cluster.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_TRANSPORT_TIMEOUT =
      new Builder(Name.MASTER_BACKUP_TRANSPORT_TIMEOUT)
          .setDefaultValue("5sec")
          .setDescription("Request timeout for backup messaging.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_BACKUP_HEARTBEAT_INTERVAL)
          .setDefaultValue("1sec")
          .setDescription("Interval at which follower updates the leader on ongoing backup.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_CONNECT_INTERVAL_MIN =
      new Builder(Name.MASTER_BACKUP_CONNECT_INTERVAL_MIN)
          .setDefaultValue("1sec")
          .setDescription("Minimum delay between each connection attempt to leader backup master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_CONNECT_INTERVAL_MAX =
      new Builder(Name.MASTER_BACKUP_CONNECT_INTERVAL_MAX)
          .setDefaultValue("10sec")
          .setDescription("Maximum delay between each connection attempt to leader backup master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_ABANDON_TIMEOUT =
      new Builder(Name.MASTER_BACKUP_ABANDON_TIMEOUT)
          .setDefaultValue("2min")
          .setDescription("Duration after which leader will abandon the backup"
              + " if not received heartbeat from backup-worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION =
      new Builder(Name.MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION)
          .setDefaultValue("0ms")
          .setDescription("Alluxio master will allow only exclusive locking of "
              + "the state-lock for this duration. This duration starts after masters "
              + "are started for the first time. "
              + "User RPCs will fail to acquire state-lock during this phase and "
              + "a backup is guaranteed take the state-lock meanwhile.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED =
      new Builder(Name.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED)
          .setDefaultValue(true)
          .setDescription("This controls whether RPCs that are waiting/holding state-lock "
              + "in shared-mode will be interrupted while state-lock is taken exclusively.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_STATE_LOCK_FORCED_DURATION =
      new Builder(Name.MASTER_BACKUP_STATE_LOCK_FORCED_DURATION)
          .setDefaultValue("15min")
          .setDescription("Exclusive locking of the state-lock will timeout after "
              + "this duration is spent on forced phase.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL =
      new Builder(Name.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL)
          .setDefaultValue("30sec")
          .setDescription("The interval at which the RPCs that are waiting/holding state-lock "
              + "in shared-mode will be interrupted while state-lock is taken exclusively.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_ENABLED =
      new Builder(Name.MASTER_DAILY_BACKUP_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether or not to enable daily primary master "
              + "metadata backup.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_FILES_RETAINED =
      new Builder(Name.MASTER_DAILY_BACKUP_FILES_RETAINED)
          .setDefaultValue(3)
          .setDescription("The maximum number of backup files to keep in the backup directory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_TIME =
      new Builder(Name.MASTER_DAILY_BACKUP_TIME)
          .setDefaultValue("05:00")
          .setDescription("Default UTC time for writing daily master metadata backups. "
              + "The accepted time format is hour:minute which is based on a 24-hour clock "
              + "(E.g., 05:30, 06:00, and 22:04). "
              + "Backing up metadata requires a pause in master metadata changes, "
              + "so please set this value to an off-peak time "
              + "to avoid interfering with other users of the system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHELL_BACKUP_STATE_LOCK_GRACE_MODE =
      new Builder(Name.MASTER_SHELL_BACKUP_STATE_LOCK_GRACE_MODE)
          .setDefaultValue("TIMEOUT")
          .setDescription("Grace mode helps taking the state-lock exclusively for backup "
              + "with minimum disruption to existing RPCs. This low-impact locking phase "
              + "is called grace-cycle. Two modes are supported: TIMEOUT/FORCED."
              + "TIMEOUT: Means exclusive locking will timeout if it cannot acquire the lock"
              + "with grace-cycle. "
              + "FORCED: Means the state-lock will be taken forcefully if grace-cycle fails "
              + "to acquire it. Forced phase might trigger interrupting of existing RPCs if "
              + "it is enabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION =
      new Builder(Name.MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION)
          .setDefaultValue("1m")
          .setDescription("The duration that controls how long the state-lock is "
              + "tried within a single grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHELL_BACKUP_STATE_LOCK_SLEEP_DURATION =
      new Builder(Name.MASTER_SHELL_BACKUP_STATE_LOCK_SLEEP_DURATION)
          .setDefaultValue("0")
          .setDescription("The duration that controls how long the lock waiter "
              + "sleeps within a single grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT =
      new Builder(Name.MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT)
          .setDefaultValue("1m")
          .setDescription("The max duration for a grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_STATE_LOCK_GRACE_MODE =
      new Builder(Name.MASTER_DAILY_BACKUP_STATE_LOCK_GRACE_MODE)
          .setDefaultValue("FORCED")
          .setDescription("Grace mode helps taking the state-lock exclusively for backup "
              + "with minimum disruption to existing RPCs. This low-impact locking phase "
              + "is called grace-cycle. Two modes are supported: TIMEOUT/FORCED."
              + "TIMEOUT: Means exclusive locking will timeout if it cannot acquire the lock"
              + "with grace-cycle. "
              + "FORCED: Means the state-lock will be taken forcefully if grace-cycle fails "
              + "to acquire it. Forced phase might trigger interrupting of existing RPCs if "
              + "it is enabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_STATE_LOCK_TRY_DURATION =
      new Builder(Name.MASTER_DAILY_BACKUP_STATE_LOCK_TRY_DURATION)
          .setDefaultValue("30s")
          .setDescription("The duration that controls how long the state-lock is "
              + "tried within a single grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_STATE_LOCK_SLEEP_DURATION =
      new Builder(Name.MASTER_DAILY_BACKUP_STATE_LOCK_SLEEP_DURATION)
          .setDefaultValue("10m")
          .setDescription("The duration that controls how long the lock waiter "
              + "sleeps within a single grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_DAILY_BACKUP_STATE_LOCK_TIMEOUT =
      new Builder(Name.MASTER_DAILY_BACKUP_STATE_LOCK_TIMEOUT)
          .setDefaultValue("12h")
          .setDescription("The max duration for a grace-cycle.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_BIND_HOST =
      new Builder(Name.MASTER_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname that Alluxio master binds to.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_CLUSTER_METRICS_UPDATE_INTERVAL =
      new Builder(Name.MASTER_CLUSTER_METRICS_UPDATE_INTERVAL)
          .setDefaultValue("1min")
          .setDescription("The interval for periodically updating the cluster level metrics.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .setIsHidden(true)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_PROXY_HOST =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_PROXY_HOST)
          .setDescription(String.format(
              "Used to bind embedded journal servers to a proxied host."
                  + "Proxy hostname will still make use of %s for bind port.",
              Name.MASTER_EMBEDDED_JOURNAL_PORT))
          // No default value for proxy-host. Server will bind to "alluxio.master.hostname"
          // as default.
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_ADDRESSES =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_ADDRESSES)
          .setDescription(String.format("A comma-separated list of journal addresses for all "
              + "masters in the cluster. The format is 'hostname1:port1,hostname2:port2,...'. When "
              + "left unset, Alluxio uses ${%s}:${%s} by default", Name.MASTER_HOSTNAME,
              Name.MASTER_EMBEDDED_JOURNAL_PORT))
          // We intentionally don't set a default value here. That way, we can use isSet() to check
          // whether the user explicitly set these addresses. If they did, we determine job master
          // embedded journal addresses using the same hostnames the user set here. Otherwise, we
          // use jobMasterHostname:jobMasterEmbeddedJournalPort by default.
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT)
          .setDescription(
              "The election timeout for the embedded journal. When this period elapses without a "
                  + "master receiving any messages, the master will attempt to become the primary."
                  + "Election timeout will be waited initially when the cluster is forming. "
                  + "So larger values for election timeout will cause longer start-up time. "
                  + "Smaller values might introduce instability to leadership.")
          .setDefaultValue("10s")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL)
          .setDescription(
              "The period between sending heartbeats from the embedded journal primary to "
                  + "followers. This should be less than half of the election timeout "
                  + String.format("{%s}", Name.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT)
                  + ", because the election is driven by heart beats.")
          .setDefaultValue("3s")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_APPENDER_BATCH_SIZE =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_APPENDER_BATCH_SIZE)
          .setDescription("Amount of data that is appended from leader to followers "
              + "in a single heartbeat. Setting higher values might require increasing "
              + "election timeout due to increased network delay. Setting lower values "
              + "might stall knowledge propagation between the leader and followers.")
          .setDefaultValue("512KB")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_PORT =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_PORT)
          .setDescription("The port to use for embedded journal communication with other masters.")
          .setDefaultValue(19200)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL)
          .setDescription("The storage level for storing embedded journal logs. Use DISK for "
              + "maximum durability. Use MAPPED for better performance, but some risk of "
              + "losing state in case of power loss or host failure. Use MEMORY for "
              + "optimal performance, but no state persistence across cluster restarts.")
          .setDefaultValue("DISK")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_SHUTDOWN_TIMEOUT =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_SHUTDOWN_TIMEOUT)
          .setDefaultValue("10sec")
          .setDescription("Maximum time to wait for embedded journal to stop on shutdown.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_WRITE_TIMEOUT =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_WRITE_TIMEOUT)
          .setDefaultValue("30sec")
          .setDescription("Maximum time to wait for a write/flush on embedded journal.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_TRIGGERED_SNAPSHOT_WAIT_TIMEOUT =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_TRIGGERED_SNAPSHOT_WAIT_TIMEOUT)
          .setDefaultValue("2hour")
          .setDescription("Maximum time to wait for the triggered snapshot to finish.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_TRANSPORT_REQUEST_TIMEOUT_MS =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_TRANSPORT_REQUEST_TIMEOUT_MS)
          .setDefaultValue("5sec")
          .setDescription("Timeout for requests between embedded journal masters.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE)
          .setDefaultValue("100MB")
          .setDescription("The maximum size of a message that can be sent to the "
              + "embedded journal server node.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_RPC_ADDRESSES =
      new Builder(Name.MASTER_RPC_ADDRESSES).setDescription(
          "A list of comma-separated host:port RPC addresses where the client should look for "
              + "masters when using multiple masters without Zookeeper. This property is not "
              + "used when Zookeeper is enabled, since Zookeeper already stores the master "
              + "addresses.")
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL =
      new Builder(Name.MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL)
          .setDefaultValue("1h")
          .setDescription("The minimum interval between files access time update journal entries "
              + "get flushed asynchronously. Setting it to a non-positive value will make the the "
              + "journal update synchronous. Asynchronous update reduces the performance impact of "
              + "tracking access time but can lose some access time update when master stops "
              + "unexpectedly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION =
      new Builder(Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION)
          .setDefaultValue("1d")
          .setDescription("The file last access time is precise up to this value. Setting it to"
              + "a non-positive value will update last access time on every file access operation."
              + "Longer precision will help reduce the performance impact of tracking access time "
              + "by reduce the amount of metadata writes occur while reading the same group of "
              + "files repetitively.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT =
      new Builder(Name.MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT)
          .setDefaultValue("1sec")
          .setDescription("Maximum time to wait for access updater to stop on shutdown.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FORMAT_FILE_PREFIX =
      new Builder(Name.MASTER_FORMAT_FILE_PREFIX)
          .setAlias("alluxio.master.format.file_prefix")
          .setDefaultValue("_format_")
          .setDescription("The file prefix of the file generated in the journal directory "
              + "when the journal is formatted. The master will search for a file with this "
              + "prefix when determining if the journal is formatted.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_STANDBY_HEARTBEAT_INTERVAL =
      new Builder(Name.MASTER_STANDBY_HEARTBEAT_INTERVAL)
          .setDefaultValue("2min")
          .setDescription("The heartbeat interval between Alluxio primary master and standby "
              + "masters.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE =
      new Builder(Name.MASTER_METASTORE)
          .setDefaultValue("HEAP")
          .setDescription("The type of metastore to use, either HEAP or ROCKS. The heap metastore "
              + "keeps all metadata on-heap, while the rocks metastore stores some metadata on "
              + "heap and some metadata on disk. The rocks metastore has the advantage of being "
              + "able to support a large namespace (1 billion plus files) without needing a "
              + "massive heap size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_DIR =
      new Builder(Name.MASTER_METASTORE_DIR)
          .setDefaultValue(String.format("${%s}/metastore", Name.WORK_DIR))
          .setDescription("The metastore work directory. Only some metastores need disk.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE =
      new Builder(Name.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE)
          // TODO(andrew): benchmark different batch sizes to improve the default and provide a
          // tuning guideline
          .setDefaultValue("1000")
          .setDescription("The batch size for evicting entries from the inode cache.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO =
      new Builder(Name.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO)
          .setDefaultValue("0.85")
          .setDescription("The high water mark for the inode cache, as a ratio from high water "
              + "mark to total cache size. If this is 0.85 and the max size is 10 million, the "
              + "high water mark value is 8.5 million. When the cache reaches the high "
              + "water mark, the eviction process will evict down to the low water mark.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO =
      new Builder(Name.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO)
          .setDefaultValue("0.8")
          .setDescription("The low water mark for the inode cache, as a ratio from low water mark "
              + "to total cache size. If this is 0.8 and the max size is 10 million, the low "
              + "water mark value is 8 million. When the cache reaches the high "
              + "water mark, the eviction process will evict down to the low water mark.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_CACHE_MAX_SIZE =
      new Builder(Name.MASTER_METASTORE_INODE_CACHE_MAX_SIZE)
          .setDefaultValue("10000000")
          .setDescription("The number of inodes to cache on-heap. "
              + "This only applies to off-heap metastores, e.g. ROCKS. Set this to 0 to disable "
              + "the on-heap inode cache")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_ITERATION_CRAWLER_COUNT =
      new Builder(Name.MASTER_METASTORE_INODE_ITERATION_CRAWLER_COUNT)
          .setDefaultSupplier(() -> Runtime.getRuntime().availableProcessors(),
              "Use {CPU core count} for enumeration")
          .setDescription("The number of threads used during inode tree enumeration.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_ENUMERATOR_BUFFER_COUNT =
      new Builder(Name.MASTER_METASTORE_INODE_ENUMERATOR_BUFFER_COUNT)
          .setDefaultValue("10000")
          .setDescription("The number of entries to buffer during read-ahead enumeration.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_ITERATOR_READAHEAD_SIZE =
      new Builder(Name.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE)
          .setDefaultValue("64MB")
          .setDescription("The read-ahead size (in bytes) for metastore iterators.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METASTORE_INODE_INHERIT_OWNER_AND_GROUP =
      new Builder(Name.MASTER_METASTORE_INODE_INHERIT_OWNER_AND_GROUP)
          .setDefaultValue("true")
          .setDescription("Whether to inherit the owner/group from the parent when creating a new "
              + "inode path if empty")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METRICS_SERVICE_THREADS =
      new Builder(Name.MASTER_METRICS_SERVICE_THREADS)
          .setDefaultValue(5)
          .setDescription("The number of threads in metrics master executor pool "
              + "for parallel processing metrics submitted by workers or clients "
              + "and update cluster metrics.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METRICS_TIME_SERIES_INTERVAL =
      new Builder(Name.MASTER_METRICS_TIME_SERIES_INTERVAL)
          .setDefaultValue("5min")
          .setDescription("Interval for which the master records metrics information. This affects "
              + "the granularity of the metrics graphed in the UI.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE)
          .setDefaultValue("100MB")
          .setDescription("The maximum size of a message that can be sent to the Alluxio master")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL =
      new Builder(Name.MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL)
          .setAlias("alluxio.master.worker.heartbeat.interval")
          .setDefaultValue("10sec")
          .setDescription("The interval between Alluxio master detections to find lost workers "
              + "and files based on updates from Alluxio workers.")
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
  public static final PropertyKey MASTER_LOCK_POOL_INITSIZE =
      new Builder(Name.MASTER_LOCK_POOL_INITSIZE)
          .setDefaultValue(1000)
          .setDescription("Initial size of the lock pool for master inodes.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOCK_POOL_LOW_WATERMARK =
      new Builder(Name.MASTER_LOCK_POOL_LOW_WATERMARK)
          .setDefaultValue(500000)
          .setDescription("Low watermark of lock pool size. "
              + "When the size grows over the high watermark, a background thread will try to "
              + "evict unused locks until the size reaches the low watermark.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOCK_POOL_HIGH_WATERMARK =
      new Builder(Name.MASTER_LOCK_POOL_HIGH_WATERMARK)
          .setDefaultValue(1000000)
          .setDescription("High watermark of lock pool size. "
              + "When the size grows over the high watermark, a background thread starts evicting "
              + "unused locks from the pool.")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOCK_POOL_CONCURRENCY_LEVEL =
      new Builder(Name.MASTER_LOCK_POOL_CONCURRENCY_LEVEL)
          .setDefaultValue(100)
          .setDescription("Maximum concurrency level for the lock pool")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS)
          .setAlias("alluxio.master.journal.flush.batch.time.ms")
          .setDefaultValue("5ms")
          .setDescription("Time to wait for batching journal writes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
      new Builder(Name.MASTER_JOURNAL_FLUSH_TIMEOUT_MS)
          .setAlias("alluxio.master.journal.flush.timeout.ms")
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
          .setDescription("The path to store master journal logs. When using the UFS journal this "
              + "could be a URI like hdfs://namenode:port/alluxio/journal. When using the embedded "
              + "journal this must be a local path")
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
  public static final PropertyKey MASTER_JOURNAL_TOLERATE_CORRUPTION =
      new Builder(Name.MASTER_JOURNAL_TOLERATE_CORRUPTION)
          .setDefaultValue(false)
          .setDescription("Whether to tolerate master state corruption "
              + "when standby master replaying journal. If enabled, errors from applying journal "
              + "to master metadata will only be logged instead of forcing master to exit. "
              + "This property should be used sparingly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setIsHidden(true)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TYPE =
      new Builder(Name.MASTER_JOURNAL_TYPE)
          .setDefaultValue("EMBEDDED")
          .setDescription("The type of journal to use. Valid options are UFS (store journal in "
              + "UFS), EMBEDDED (use a journal embedded in the masters), and NOOP (do not use a "
              + "journal)")
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
          .setAlias("alluxio.master.journal.tailer.shutdown.quiet.wait.time.ms")
          .setDefaultValue("5sec")
          .setDescription("Before the standby master shuts down its tailer thread, there "
              + "should be no update to the leader master's journal in this specified time "
              + "period.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
      new Builder(Name.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS)
          .setAlias("alluxio.master.journal.tailer.sleep.time.ms")
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
          .setAlias("alluxio.master.journal.gc.period.ms")
          .setDefaultValue("2min")
          .setDescription("Frequency with which to scan for and delete stale journal checkpoints.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_GC_THRESHOLD_MS)
          .setAlias("alluxio.master.journal.gc.threshold.ms")
          .setDefaultValue("5min")
          .setDescription("Minimum age for garbage collecting checkpoints.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
      new Builder(Name.MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS)
          .setAlias("alluxio.master.journal.temporary.file.gc.threshold.ms")
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
          .setDefaultValue("1s")
          .setDescription("How often the master checks persistence status for files written using "
              + "ASYNC_THROUGH")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_INITIAL_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS)
          .setDefaultValue("1s")
          .setDescription("How often the  master persistence checker checks persistence status "
              + "for files written using ASYNC_THROUGH")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_MAX_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_MAX_INTERVAL_MS)
          .setDefaultValue("1hr")
          .setDescription("Max wait interval for master persistence checker persistence status "
              + "for files written using ASYNC_THROUGH")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS =
      new Builder(Name.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS)
          .setDefaultValue("1day")
          .setDescription("Total wait time for master persistence checker persistence status "
              + "for files written using ASYNC_THROUGH")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS =
      new Builder(Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS)
          .setDefaultValue("1s")
          .setDescription("How often the master schedules persistence jobs "
              + "for files written using ASYNC_THROUGH")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PERSISTENCE_BLACKLIST =
      new Builder(Name.MASTER_PERSISTENCE_BLACKLIST)
          .setDescription("Patterns to blacklist persist, comma separated, string match, no regex."
            + " This affects any async persist call (including ASYNC_THROUGH writes and CLI "
            + "persist) but does not affect CACHE_THROUGH writes. Users may want to specify "
            + "temporary files in the blacklist to avoid unnecessary I/O and errors. Some "
            + "examples are `.staging` and `.tmp`.")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_REPLICATION_CHECK_INTERVAL_MS =
      new Builder(Name.MASTER_REPLICATION_CHECK_INTERVAL_MS)
          .setDefaultValue("1min")
          .setDescription("How often the master runs background process to check replication "
              + "level for files")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_PRINCIPAL = new Builder(Name.MASTER_PRINCIPAL)
      .setDescription("Kerberos principal for Alluxio master.")
      .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
      .setScope(Scope.MASTER)
      .build();
  public static final PropertyKey MASTER_RPC_PORT =
      new Builder(Name.MASTER_RPC_PORT)
          .setAlias("alluxio.master.port")
          .setDefaultValue(19998)
          .setDescription("The port for Alluxio master's RPC service.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey MASTER_SERVING_THREAD_TIMEOUT =
      new Builder(Name.MASTER_SERVING_THREAD_TIMEOUT)
          .setDefaultValue("5m")
          .setDescription("When stepping down from being the primary, the master will wait this "
              + "long for the gRPC serving thread to stop before giving up and shutting down "
              + "the server")
          .setIsHidden(true)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SKIP_ROOT_ACL_CHECK =
      new Builder(Name.MASTER_SKIP_ROOT_ACL_CHECK)
          .setDefaultValue(false)
          .setDescription("Skip root directory ACL check when restarting either from journal or "
              + "backup. This is to allow users to restore a backup from a different cluster onto "
              + "their current one without having to recreate the different clusters owner user.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .setIsHidden(true)
          .setIgnoredSiteProperty(true)
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
  public static final PropertyKey MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE =
      new Builder(Name.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE)
          .setDefaultValue("MEM, SSD, HDD")
          .setDescription("The list of medium types we support in the system.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_TTL_CHECKER_INTERVAL_MS =
      new Builder(Name.MASTER_TTL_CHECKER_INTERVAL_MS)
          .setAlias("alluxio.master.ttl.checker.interval.ms")
          .setDefaultValue("1hour")
          .setDescription("How often to periodically check and delete the files "
              + "with expired ttl value.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_INTERVAL =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_INTERVAL)
          .setDefaultValue("30sec")
          .setDescription("Time interval to periodically actively sync UFS")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_MAX_AGE =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_MAX_AGE)
          .setDefaultValue("10")
          .setDescription("The maximum number of intervals we will wait to find a quiet "
            + "period before we have to sync the directories")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_INITIAL_SYNC_ENABLED =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_INITIAL_SYNC_ENABLED)
          .setDefaultValue("true")
          .setDescription("Whether to perform an initial sync when we add a sync point")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .setIsHidden(true)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_MAX_ACTIVITIES =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_MAX_ACTIVITIES)
          .setDefaultValue("10")
          .setDescription("Max number of changes in a directory "
              + "to be considered for active syncing")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_THREAD_POOL_SIZE =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_THREAD_POOL_SIZE)
          .setDefaultSupplier(() -> Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
              "The number of threads used by the active sync provider process active sync events."
                  + " A higher number allow the master to use more CPU to process events from "
                  + "an event stream in parallel. If this value is too low, Alluxio may fall "
                  + "behind processing events. Defaults to # of processors / 2")
          .setDescription("Max number of threads used to perform active sync")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT)
          .setDefaultValue("10sec")
          .setDescription("Max time to wait before timing out a polling operation")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_EVENT_RATE_INTERVAL =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_EVENT_RATE_INTERVAL)
          .setDefaultValue("60sec")
          .setDescription("The time interval we use to estimate incoming event rate")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT)
          .setDefaultValue("10sec")
          .setDescription("The max total duration to retry failed active sync operations."
              + "A large duration is useful to handle transient failures such as an "
              + "unresponsive under storage but can lock the inode tree being synced longer.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();

  public static final PropertyKey MASTER_UFS_ACTIVE_SYNC_POLL_BATCH_SIZE =
      new Builder(Name.MASTER_UFS_ACTIVE_SYNC_POLL_BATCH_SIZE)
          .setDefaultValue("1024")
          .setDescription("The number of event batches that should be submitted together to a "
              + "single thread for processing.")
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
  public static final PropertyKey MASTER_UFS_MANAGED_BLOCKING_ENABLED =
      new Builder(Name.MASTER_UFS_MANAGED_BLOCKING_ENABLED)
          .setDescription("Whether to run UFS operations with managed blocking. "
              + "This will provide RPC layer a hint that UFS is possible slow."
              + "The default is true for object stores and false for the rest. "
              + "unless set explicitly.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .setIsHidden(true)
          .build();
  public static final PropertyKey MASTER_UFS_PATH_CACHE_CAPACITY =
      new Builder(Name.MASTER_UFS_PATH_CACHE_CAPACITY)
          .setDefaultValue(100000)
          .setDescription("The capacity of the UFS path cache. This cache is used to "
              + "approximate the `ONCE` metadata load behavior (see "
              + "`alluxio.user.file.metadata.load.type`). Larger caches will consume more "
              + "memory, but will better approximate the `ONCE` behavior.")
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
              + "`alluxio.user.file.metadata.load.type=ONCE` will behave like `ALWAYS`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UPDATE_CHECK_ENABLED =
      new Builder(Name.MASTER_UPDATE_CHECK_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to check for update availability.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UPDATE_CHECK_INTERVAL =
      new Builder(Name.MASTER_UPDATE_CHECK_INTERVAL)
          .setDefaultValue("7day")
          .setDescription("The interval to check for update availability.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED =
      new Builder(Name.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED)
          .setDefaultValue(true)
          .setDescription("When set to false, writing files using ASYNC_THROUGH or persist CLI "
              + "with object stores as the UFS will first create temporary objects "
              + "suffixed by \".alluxio.TIMESTAMP.tmp\" in the object store before "
              + "committed to the final UFS path. When set to true, files will be "
              + "put to the destination path directly in the object store without staging "
              + "with a temp suffix. Enabling this optimization by directly persisting files "
              + "can significantly improve the efficiency writing to object store by making less "
              + "data copy as rename in object store can be slow, "
              + "but leaving a short vulnerability window for undefined behavior if a file "
              + "is written using ASYNC_THROUGH but renamed or removed before the async "
              + "persist operation completes, while this same file path was reused for other new "
              + "files in Alluxio.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WEB_BIND_HOST =
      new Builder(Name.MASTER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname Alluxio master web UI binds to.")
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
  public static final PropertyKey MASTER_WORKER_INFO_CACHE_REFRESH_TIME =
      new Builder(Name.MASTER_WORKER_INFO_CACHE_REFRESH_TIME)
          .setDefaultValue("10sec")
          .setDescription("The worker information list will be refreshed "
              + "after being cached for this time period. If the refresh time is too big, "
              + "operations on the job servers or clients may fail because of "
              + "the stale worker info. If it is too small, "
              + "continuously updating worker information may case lock contention "
              + "in the block master")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_WORKER_TIMEOUT_MS =
      new Builder(Name.MASTER_WORKER_TIMEOUT_MS)
          .setAlias("alluxio.master.worker.timeout.ms")
          .setDefaultValue("5min")
          .setDescription("Timeout between master and worker indicating a lost worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_METADATA_SYNC_CONCURRENCY_LEVEL =
      new Builder(Name.MASTER_METADATA_SYNC_CONCURRENCY_LEVEL)
          .setDefaultValue(6)
          .setDescription("The maximum number of concurrent sync tasks running for a given sync "
              + "operation")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE =
      new Builder(Name.MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE)
          .setDefaultSupplier(() -> Runtime.getRuntime().availableProcessors(),
              "The total number of threads which can concurrently execute metadata sync "
                  + "operations.")
          .setDescription("The number of threads used to execute all metadata sync"
              + "operations")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_METADATA_SYNC_UFS_PREFETCH_POOL_SIZE =
      new Builder(Name.MASTER_METADATA_SYNC_UFS_PREFETCH_POOL_SIZE)
          .setDefaultSupplier(() -> Runtime.getRuntime().availableProcessors(),
              "The number of threads which can concurrently fetch metadata from UFSes during a "
                  + "metadata sync operations")
          .setDescription("The number of threads used to fetch UFS objects for all metadata sync"
              + "operations")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_RPC_EXECUTOR_PARALLELISM =
      new Builder(Name.MASTER_RPC_EXECUTOR_PARALLELISM)
          .setDefaultSupplier(() -> 2 * Runtime.getRuntime().availableProcessors(),
              "2 * {CPU core count}")
          .setDescription("The parallelism level of master RPC executor service .")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_RPC_EXECUTOR_MIN_RUNNABLE =
      new Builder(Name.MASTER_RPC_EXECUTOR_MIN_RUNNABLE)
          .setDefaultValue(1)
          .setDescription("the minimum allowed number of core threads not blocked. "
              + "To ensure progress, when too few unblocked threads exist and unexecuted tasks may "
              + "exist, new threads are constructed up to the value of "
              + Name.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE
              + ". A value of 1 ensures liveness. A larger value might improve "
              + "throughput but might also increase overhead.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_RPC_EXECUTOR_CORE_POOL_SIZE =
      new Builder(Name.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE)
          .setDefaultValue(0)
          .setDescription("the number of threads to keep in thread pool of master RPC executor "
              + "service. By default it is same as the parallelism level, but may be "
              + "set to a larger value to reduce dynamic overhead if tasks regularly block. "
              + "A smaller value (for example 0) is equivalent to the default.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_RPC_EXECUTOR_MAX_POOL_SIZE =
      new Builder(Name.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE)
          .setDefaultValue(500)
          .setDescription("the maximum number of threads allowed for master RPC executor service."
              + " When the maximum is reached, attempts to replace blocked threads fail.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_RPC_EXECUTOR_KEEPALIVE =
      new Builder(Name.MASTER_RPC_EXECUTOR_KEEPALIVE)
          .setDefaultValue("60sec")
          .setDescription("the keep alive time of a thread in master RPC executor service"
              + "last used before this thread is terminated (and replaced if necessary).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();

  //
  // Secondary master related properties
  //
  public static final PropertyKey SECONDARY_MASTER_METASTORE_DIR =
      new Builder(Name.SECONDARY_MASTER_METASTORE_DIR)
          .setDefaultValue(String.format("${%s}/secondary-metastore", Name.WORK_DIR))
          .setDescription(
              "The secondary master metastore work directory. Only some metastores need disk.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();

  //
  // File system master related properties
  //
  public static final PropertyKey MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE =
      new Builder(Name.MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE)
          .setDefaultValue(10000)
          .setDescription(
              "Count of items on each list-status response message.")
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
          .setDescription("The hostname Alluxio's worker node binds to.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)
          .setAlias("alluxio.worker.block.heartbeat.interval.ms")
          .setDefaultValue("1sec")
          .setDescription("The interval between block workers' heartbeats to update "
              + "block status, storage health and other workers' information to Alluxio Master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
      new Builder(Name.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS)
          .setAlias("alluxio.worker.block.heartbeat.timeout.ms")
          .setDefaultValue(String.format("${%s}", Name.WORKER_MASTER_CONNECT_RETRY_TIMEOUT))
          .setDescription("The timeout value of block workers' heartbeats. If the worker can't "
              + "connect to master before this interval expires, the worker will exit.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_CONTAINER_HOSTNAME =
      new Builder(Name.WORKER_CONTAINER_HOSTNAME)
          .setDescription("The container hostname if worker is running in a container.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
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
  public static final PropertyKey WORKER_DATA_SERVER_CLASS =
      new Builder(Name.WORKER_DATA_SERVER_CLASS)
          .setDefaultValue("alluxio.worker.grpc.GrpcDataServer")
          .setDescription("Selects the networking stack to run the worker with. Valid options "
              + "are: `alluxio.worker.grpc.GrpcDataServer`.")
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
              + "path for the domain socket with be {path}/{uuid}.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID =
      new Builder(Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)
          .setDefaultValue("false")
          .setDescription("If true, the property " + Name.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS
              + "is the path to the home directory for the domain socket and a unique identifier "
              + "is used as the domain socket name. If false, the property is the absolute path "
              + "to the UNIX domain socket.")
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
              + "${alluxio.worker.data.tmp.folder}.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  /**
   * @deprecated use block annotators instead
   */
  @Deprecated(message = "Use WORKER_BLOCK_ANNOTATOR_CLASS instead.")
  public static final PropertyKey WORKER_EVICTOR_CLASS =
      new Builder(Name.WORKER_EVICTOR_CLASS)
          .setDescription("The strategy that a worker uses to evict block files when a "
              + "storage layer runs out of space. Valid options include "
              + "`alluxio.worker.block.evictor.LRFUEvictor`, "
              + "`alluxio.worker.block.evictor.GreedyEvictor`, "
              + "`alluxio.worker.block.evictor.LRUEvictor`, "
              + "`alluxio.worker.block.evictor.PartialLRUEvictor`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_ANNOTATOR_CLASS =
      new Builder(Name.WORKER_BLOCK_ANNOTATOR_CLASS)
          .setDefaultValue("alluxio.worker.block.annotator.LRUAnnotator")
          .setDescription("The strategy that a worker uses to annotate blocks "
              + "in order to have an ordered view of them during internal"
              + "management tasks such as eviction and promotion/demotion. "
              + " Valid options include: "
              + "`alluxio.worker.block.annotator.LRFUAnnotator`, "
              + "`alluxio.worker.block.annotator.LRUAnnotator`, ")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_ANNOTATOR_LRFU_ATTENUATION_FACTOR =
      new Builder(Name.WORKER_BLOCK_ANNOTATOR_LRFU_ATTENUATION_FACTOR)
          .setDefaultValue(2.0)
          .setDescription(
              "A attenuation factor in [2, INF) to control the behavior of LRFU annotator.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR =
      new Builder(Name.WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR)
          .setDefaultValue(0.25)
          .setDescription("A factor in [0, 1] to control the behavior of LRFU: smaller value "
              + "makes LRFU more similar to LFU; and larger value makes LRFU closer to LRU.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_BACKOFF_STRATEGY =
      new Builder(Name.WORKER_MANAGEMENT_BACKOFF_STRATEGY)
          .setDefaultValue("ANY")
          .setDescription("Defines the backoff scope respected by background tasks. "
              + "Supported values are ANY / DIRECTORY. "
              + "ANY: Management tasks will backoff from worker when there is any user I/O."
              + "This mode will ensure low management task overhead in order to favor "
              + "immediate user I/O performance. However, making progress on management tasks "
              + "will require quite periods on the worker."
              + "DIRECTORY: Management tasks will backoff from directories with ongoing user I/O."
              + "This mode will give better chance of making progress on management tasks."
              + "However, immediate user I/O throughput might be reduced due to "
              + "increased management task activity.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME =
      new Builder(Name.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME)
          .setDefaultValue("10sec")
          .setDescription("Management tasks will not run for this long after load detected. "
              + "Any user I/O will still register as a load for this period of time after "
              + "it is finished. Short durations might cause interference between user I/O "
              + "and background tier management tasks. Long durations might cause "
              + "starvation for background tasks.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES =
      new Builder(Name.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES)
          .setDefaultValue("1GB")
          .setDescription("The amount of space that is reserved from each storage directory "
              + "for internal management tasks.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TASK_THREAD_COUNT =
      new Builder(Name.WORKER_MANAGEMENT_TASK_THREAD_COUNT)
          .setDefaultSupplier(() -> Runtime.getRuntime().availableProcessors(),
              "Use {CPU core count} threads for all management tasks")
          .setDescription("The number of threads for management task executor")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_BLOCK_TRANSFER_CONCURRENCY_LIMIT =
      new Builder(Name.WORKER_MANAGEMENT_BLOCK_TRANSFER_CONCURRENCY_LIMIT)
          .setDefaultSupplier(() -> Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
              "Use {CPU core count}/2 threads block transfer")
          .setDescription("Puts a limit to how many block transfers are "
              + "executed concurrently during management.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_ALIGN_ENABLED =
      new Builder(Name.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to align tiers based on access pattern.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED =
      new Builder(Name.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to promote blocks to higher tiers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_SWAP_RESTORE_ENABLED =
      new Builder(Name.WORKER_MANAGEMENT_TIER_SWAP_RESTORE_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to run management swap-restore task when "
              + "tier alignment cannot make progress.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_ALIGN_RANGE =
      new Builder(Name.WORKER_MANAGEMENT_TIER_ALIGN_RANGE)
          .setDefaultValue(100)
          .setDescription(
              "Maximum number of blocks to consider from one tier for a single alignment task.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_PROMOTE_RANGE =
      new Builder(Name.WORKER_MANAGEMENT_TIER_PROMOTE_RANGE)
          .setDefaultValue(100)
          .setDescription(
              "Maximum number of blocks to consider from one tier for a single promote task.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT =
      new Builder(Name.WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT)
          .setDefaultValue(90)
          .setDescription("Max percentage of each tier that could be used for promotions. "
              + "Promotions will be stopped to a tier once its used space go over this value. "
              + "(0 means never promote, and, 100 means always promote.")
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
  public static final PropertyKey WORKER_MASTER_CONNECT_RETRY_TIMEOUT =
      new Builder(Name.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)
          .setDescription("Retry period before workers give up on connecting to master and exit.")
          .setDefaultValue("1hour")
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
  public static final PropertyKey WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX)
          .setDefaultValue(8)
          .setDescription("The maximum number of threads used to cache blocks asynchronously in "
              + "the data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_BLOCK_READER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_BLOCK_READER_THREADS_MAX)
          .setDefaultValue(2048)
          .setDescription("The maximum number of threads used to read blocks in the data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_BLOCK_WRITER_THREADS_MAX =
      new Builder(Name.WORKER_NETWORK_BLOCK_WRITER_THREADS_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of threads used to write blocks in the data server.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES =
      new Builder(Name.WORKER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES)
          .setDefaultValue(8)
          .setDescription("When a client writes to a remote worker, the maximum number of "
              + "data messages to buffer by the server for each request.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_FLOWCONTROL_WINDOW =
      new Builder(Name.WORKER_NETWORK_FLOWCONTROL_WINDOW)
          .setDefaultValue("2MB")
          .setDescription("The HTTP2 flow control window used by worker gRPC connections. Larger "
              + "value will allow more data to be buffered but will use more memory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_KEEPALIVE_TIME_MS =
      new Builder(Name.WORKER_NETWORK_KEEPALIVE_TIME_MS)
          .setDefaultValue("30sec")
          .setDescription("The amount of time for data server (for block reads and block writes) "
              + "to wait for a response before pinging the client to see if it is still alive.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_KEEPALIVE_TIMEOUT_MS =
      new Builder(Name.WORKER_NETWORK_KEEPALIVE_TIMEOUT_MS)
          .setDefaultValue("30sec")
          .setDescription("The maximum time for a data server (for block reads and block writes) "
              + "to wait for a keepalive response before closing the connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.WORKER_NETWORK_MAX_INBOUND_MESSAGE_SIZE)
          .setDefaultValue("4MB")
          .setDescription("The max inbound message size used by worker gRPC connections.")
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
  public static final PropertyKey WORKER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.WORKER_NETWORK_NETTY_CHANNEL)
          .setDescription("Netty channel type: NIO or EPOLL. If EPOLL is not available, this will "
              + "automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .setDefaultValue("EPOLL")
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
  public static final PropertyKey WORKER_NETWORK_READER_BUFFER_SIZE_BYTES =
      new Builder(Name.WORKER_NETWORK_READER_BUFFER_SIZE_BYTES)
          .setDefaultValue("4MB")
          .setDescription("When a client reads from a remote worker, the maximum amount of data"
              + " not received by client allowed before the worker pauses sending more data."
              + " If this value is lower than read chunk size, read performance may be impacted"
              + " as worker waits more often for buffer to free up. Higher value will increase"
              + " the memory consumed by each read request.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_READER_MAX_CHUNK_SIZE_BYTES =
      new Builder(Name.WORKER_NETWORK_READER_MAX_CHUNK_SIZE_BYTES)
          .setDefaultValue("2MB")
          .setDescription("When a client read from a remote worker, the maximum chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_SHUTDOWN_TIMEOUT =
      new Builder(Name.WORKER_NETWORK_SHUTDOWN_TIMEOUT)
          .setDefaultValue("15sec")
          .setDescription("Maximum amount of time to wait until the worker gRPC server "
              + "is shutdown (regardless of the quiet period).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_NETWORK_ZEROCOPY_ENABLED =
      new Builder(Name.WORKER_NETWORK_ZEROCOPY_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether zero copy is enabled on worker when processing data streams.")
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
          .setAlias("alluxio.worker.port")
          .setDefaultValue(29999)
          .setDescription("The port for Alluxio worker's RPC service.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey WORKER_SESSION_TIMEOUT_MS =
      new Builder(Name.WORKER_SESSION_TIMEOUT_MS)
          .setAlias("alluxio.worker.session.timeout.ms")
          .setDefaultValue("1min")
          .setDescription("Timeout between worker and client connection "
              + "indicating a lost session connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_STORAGE_CHECKER_ENABLED =
      new Builder(Name.WORKER_STORAGE_CHECKER_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether periodic storage health checker is enabled on Alluxio workers.")
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
  public static final PropertyKey WORKER_TIERED_STORE_FREE_AHEAD_BYTES =
      new Builder(Name.WORKER_TIERED_STORE_FREE_AHEAD_BYTES)
          .setDefaultValue(0)
          .setDescription("Amount to free ahead when worker storage is full. "
              + "Higher values will help decrease CPU utilization under peak storage. "
              + "Lower values will increase storage utilization.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  // TODO(binfan): Use alluxio.worker.tieredstore.level0.dirs.mediumtype instead
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_ALIAS =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_ALIAS, 0)
          .setDefaultValue("MEM")
          .setDescription("The alias of the top storage tier on this worker. It must "
              + "match one of the global storage tiers from the master configuration. We "
              + "disable placing an alias lower in the global hierarchy before an alias with "
              + "a higher position on the worker hierarchy. So by default, SSD cannot come "
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
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE, 0)
          .setDefaultValue(
              String.format("${%s}", Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0)))
          .setDescription(String.format(
              "A list of media types (e.g., \"MEM,SSD,SSD\") for each storage "
                  + "directory on the top storage tier specified by %s.",
              PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH.mName))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 0)
          .setDefaultValue(String.format("${%s}", Name.WORKER_MEMORY_SIZE))
          .setDescription("The capacity of the top storage tier.")
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
  // TODO(binfan): Use alluxio.worker.tieredstore.level1.dirs.mediumtype instead"
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
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_MEDIUMTYPE =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE, 1)
          .setDefaultValue(
              String.format("${%s}", Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1)))
          .setDescription(String.format(
              "A list of media types (e.g., \"MEM,SSD,SSD\") for each storage "
                  + "directory on the second storage tier specified by %s.",
              PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH.mName))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 1)
          .setDescription("The capacity of the second storage tier.")
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
  //TODO(binfan): Use alluxio.worker.tieredstore.level2.dirs.mediumtype instead"
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
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_MEDIUMTYPE =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE, 2)
          .setDefaultValue(
              String.format("${%s}", Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(2)))
          .setDescription(String.format(
              "A list of media types (e.g., \"MEM,SSD,SSD\") for each storage "
                  + "directory on the third storage tier specified by %s.",
              PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH.mName))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA =
      new Builder(Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA, 2)
          .setDescription("The capacity of the third storage tier.")
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
  public static final PropertyKey WORKER_WEB_BIND_HOST =
      new Builder(Name.WORKER_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname Alluxio worker's web server binds to.")
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
          .setAlias("alluxio.worker.ufs.block.open.timeout.ms")
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
              + "`ASYNC_THROUGH` (try to cache, write to UnderFS asynchronously), "
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
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey PROXY_STREAM_CACHE_TIMEOUT_MS =
      new Builder(Name.PROXY_STREAM_CACHE_TIMEOUT_MS)
          .setAlias("alluxio.proxy.stream.cache.timeout.ms")
          .setDefaultValue("1hour")
          .setDescription("The timeout for the input and output streams cache eviction in the "
              + "proxy.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.NONE)
          .build();
  public static final PropertyKey PROXY_WEB_BIND_HOST =
      new Builder(Name.PROXY_WEB_BIND_HOST)
          .setDefaultValue("0.0.0.0")
          .setDescription("The hostname that the Alluxio proxy's web server runs on.")
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey LOCALITY_SCRIPT =
      new Builder(Name.LOCALITY_SCRIPT)
          .setDefaultValue(Constants.ALLUXIO_LOCALITY_SCRIPT)
          .setDescription("A script to determine tiered identity for locality checking")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey LOCALITY_TIER_NODE =
      new Builder(Template.LOCALITY_TIER, Constants.LOCALITY_NODE)
          .setDescription("Value to use for determining node locality")
          .setScope(Scope.ALL)
          .build();
  // This property defined so that it is included in the documentation.
  public static final PropertyKey LOCALITY_TIER_RACK =
      new Builder(Template.LOCALITY_TIER, Constants.LOCALITY_RACK)
          .setDescription("Value to use for determining rack locality")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();

  public static final PropertyKey LOCALITY_COMPARE_NODE_IP =
          new Builder(Name.LOCALITY_COMPARE_NODE_IP)
          .setDefaultValue(false)
          .setDescription("Whether try to resolve the node IP address for locality checking")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
          .build();

  //
  // Log server related properties
  //
  // Used in alluxio-config.sh and conf/log4j.properties
  public static final PropertyKey LOGSERVER_LOGS_DIR =
      new Builder(Name.LOGSERVER_LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", Name.WORK_DIR))
          .setDescription("Default location for remote log files.")
          .setIgnoredSiteProperty(true)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  // Used in alluxio-config.sh and conf/log4j.properties
  public static final PropertyKey LOGSERVER_HOSTNAME =
      new Builder(Name.LOGSERVER_HOSTNAME)
          .setDescription("The hostname of Alluxio logserver.")
          .setIgnoredSiteProperty(true)
          .setScope(Scope.SERVER)
          .build();
  // Used in alluxio-config.sh and conf/log4j.properties
  public static final PropertyKey LOGSERVER_PORT =
      new Builder(Name.LOGSERVER_PORT)
          .setDefaultValue(45600)
          .setDescription("Default port of logserver to receive logs from alluxio servers.")
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
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MIN =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MIN)
          .setDefaultValue(0)
          .setDescription("The minimum number of block master clients cached in the block master "
              + "client pool. For long running processes, this should be set to zero.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX)
          .setDefaultValue(10)
          .setDescription("The maximum number of block master clients cached in the block master "
              + "client pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setAlias(new String[] {"alluxio.user.block.master.client.threads"})
          .build();
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_POOL_GC_INTERVAL_MS =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_POOL_GC_INTERVAL_MS)
          .setDefaultValue("120sec")
          .setDescription("The interval at which block master client GC checks occur.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_MASTER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_BLOCK_MASTER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setDefaultValue("120sec")
          .setDescription("A block master client is closed if it has been idle for more than this "
              + "threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_MIN =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_MIN)
          .setDefaultValue(0)
          .setDescription("The minimum number of block worker clients cached in the block "
              + "worker client pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setIsHidden(true)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_MAX =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_MAX)
          .setDefaultValue(1024)
          .setDescription("The maximum number of block worker clients cached in the block "
              + "worker client pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setAlias(new String[] {"alluxio.user.block.worker.client.pool.size"})
          .build();
  public static final PropertyKey USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setDefaultValue("300sec")
          .setDescription("A block worker client is closed if it has been idle for more than this "
              + "threshold.")
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
  public static final PropertyKey USER_CONF_SYNC_INTERVAL =
      new Builder(Name.USER_CONF_SYNC_INTERVAL)
          .setDefaultValue("1min")
          .setDescription("The time period of client master heartbeat to "
              + "update the configuration if necessary from meta master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_REPLICATION_MAX =
      new Builder(Name.USER_FILE_REPLICATION_MAX)
          .setDefaultValue(-1)
          .setDescription("The target max replication level of a file in Alluxio space. Setting "
              + "this property to a negative value means no upper limit.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_REPLICATION_MIN =
      new Builder(Name.USER_FILE_REPLICATION_MIN)
          .setDefaultValue(0)
          .setDescription("The target min replication level of a file in Alluxio space.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_REPLICATION_DURABLE =
      new Builder(Name.USER_FILE_REPLICATION_DURABLE)
          .setDefaultValue(1)
          .setDescription("The target replication level of a file created by ASYNC_THROUGH writes"
              + "before this file is persisted.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_SEQUENTIAL_PREAD_THRESHOLD =
      new Builder(Name.USER_FILE_SEQUENTIAL_PREAD_THRESHOLD)
          .setDefaultValue("2MB")
          .setDescription("An upper bound on the client buffer size for positioned read to hint "
              + "at the sequential nature of reads. For reads with a buffer size greater than this "
              + "threshold, the read op is treated to be sequential and the worker may handle the "
              + "read differently. For instance, cold reads from the HDFS ufs may use a different "
              + "HDFS client API.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_TARGET_MEDIA =
      new Builder(Name.USER_FILE_TARGET_MEDIA)
          .setDescription("Preferred media type while storing file's blocks.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_SIZE_BYTES_DEFAULT =
      new Builder(Name.USER_BLOCK_SIZE_BYTES_DEFAULT)
          .setDefaultValue("64MB")
          .setDescription("Default block size for Alluxio files.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_READ_RETRY_SLEEP_MIN =
          new Builder(Name.USER_BLOCK_READ_RETRY_SLEEP_MIN)
          .setDefaultValue("250ms")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_READ_RETRY_SLEEP_MAX =
          new Builder(Name.USER_BLOCK_READ_RETRY_SLEEP_MAX)
          .setDefaultValue("2sec")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_READ_RETRY_MAX_DURATION =
      new Builder(Name.USER_BLOCK_READ_RETRY_MAX_DURATION)
          .setDefaultValue("2min")
          .build();
  // TODO(cc): remove this since configuration propagation is always enabled?
  public static final PropertyKey USER_CONF_CLUSTER_DEFAULT_ENABLED =
      new Builder(Name.USER_CONF_CLUSTER_DEFAULT_ENABLED)
          .setDefaultValue(true)
          .setDescription("When this property is true, an Alluxio client will load the default "
              + "values of configuration properties set by Alluxio master.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_DATE_FORMAT_PATTERN =
      new Builder(Name.USER_DATE_FORMAT_PATTERN)
          .setDefaultValue("MM-dd-yyyy HH:mm:ss:SSS")
          .setDescription("Display formatted date in cli command and web UI by given date "
              + "format pattern.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_DIRECT_MEMORY_IO_ENABLED =
      new Builder(Name.USER_DIRECT_MEMORY_IO_ENABLED)
          .setDefaultValue(false)
          .setIsHidden(true)
          .setDescription("(Experimental) If this is enabled, when clients read from local "
              + "worker, they read the block directly from the first directory of the first "
              + "tier of that worker. Note this optimization can be unsafe.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_BUFFER_BYTES =
      new Builder(Name.USER_FILE_BUFFER_BYTES)
          .setDefaultValue("8MB")
          .setDescription("The size of the file buffer to use for file system reads/writes.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_RESERVED_BYTES =
      new Builder(Name.USER_FILE_RESERVED_BYTES)
          .setDefaultValue(String.format("${%s}", Name.USER_BLOCK_SIZE_BYTES_DEFAULT))
          .setDescription("The size to reserve on workers for file system writes."
              + "Using smaller value will improve concurrency for writes smaller than block size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_COPYFROMLOCAL_BLOCK_LOCATION_POLICY =
      new Builder(Name.USER_FILE_COPYFROMLOCAL_BLOCK_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.block.policy.RoundRobinPolicy")
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
  public static final PropertyKey USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN =
      new Builder(Name.USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN)
          .setDefaultValue(0)
          .setDescription("The minimum number of fs master clients cached in the fs master "
              + "client pool. For long running processes, this should be set to zero.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX =
      new Builder(Name.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX)
          .setDefaultValue(10)
          .setDescription("The maximum number of fs master clients cached in the fs master "
              + "client pool.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setAlias(new String[] {"alluxio.user.file.master.client.threads"})
          .build();
  public static final PropertyKey USER_FILE_MASTER_CLIENT_POOL_GC_INTERVAL_MS =
      new Builder(Name.USER_FILE_MASTER_CLIENT_POOL_GC_INTERVAL_MS)
          .setDefaultValue("120sec")
          .setDescription("The interval at which file system master client GC checks occur.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS =
      new Builder(Name.USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS)
          .setDefaultValue("120sec")
          .setDescription("A fs master client is closed if it has been idle for more than this "
              + "threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_METADATA_LOAD_TYPE =
      new Builder(Name.USER_FILE_METADATA_LOAD_TYPE)
          .setDefaultValue("ONCE")
          .setDescription("The behavior of loading metadata from UFS. When information about "
              + "a path is requested and the path does not exist in Alluxio, metadata can be "
              + "loaded from the UFS. Valid options are `ALWAYS`, `NEVER`, and `ONCE`. "
              + "`ALWAYS` will always access UFS to see if the path exists in the UFS. "
              + "`NEVER` will never consult the UFS. `ONCE` will access the UFS the \"first\" "
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
  public static final PropertyKey USER_FILE_READ_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_READ_TYPE_DEFAULT)
          .setDefaultValue("CACHE")
          .setDescription("Default read type when creating Alluxio files. Valid options are "
              + "`CACHE_PROMOTE` (move data to highest tier if already in Alluxio storage, "
              + "write data into highest tier of local Alluxio if data needs to be read from "
              + "under storage), `CACHE` (write data into highest tier of local Alluxio if "
              + "data needs to be read from under storage), `NO_CACHE` (no data interaction "
              + "with Alluxio, if the read is from Alluxio data migration or eviction will "
              + "not occur).")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_PERSIST_ON_RENAME =
      new Builder(Name.USER_FILE_PERSIST_ON_RENAME)
          .setDefaultValue("false")
          .setDescription("Whether or not to asynchronously persist any files which have been "
              + "renamed. This is helpful when working with compute frameworks which use rename "
              + "to commit results.")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME =
      new Builder(Name.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME)
          .setDefaultValue("0")
          .setDescription(String.format("Time to wait before starting the persistence job. "
              + "When the value is set to -1, the file will be persisted by rename operation "
              + "or persist CLI but will not be automatically persisted in other cases. "
              + "This is to avoid the heavy object copy in rename operation when %s is set to %s. "
              + "This value should be smaller than the value of %s",
              Name.USER_FILE_WRITE_TYPE_DEFAULT, WritePType.ASYNC_THROUGH,
              Name.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS))
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WAITCOMPLETED_POLL_MS =
      new Builder(Name.USER_FILE_WAITCOMPLETED_POLL_MS)
          .setAlias("alluxio.user.file.waitcompleted.poll.ms")
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
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_CREATE_TTL_ACTION =
      new Builder(Name.USER_FILE_CREATE_TTL_ACTION)
          .setDefaultValue("DELETE")
          .setDescription("When file's ttl is expired, the action performs on it. Options: "
              + "DELETE (default) or FREE")
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_UFS_TIER_ENABLED =
      new Builder(Name.USER_FILE_UFS_TIER_ENABLED)
          .setDescription("When workers run out of available memory, whether the client can skip "
              + "writing data to Alluxio but fallback to write to UFS without stopping the "
              + "application. This property only works when the write type is ASYNC_THROUGH.")
          .setDefaultValue(false)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_WRITE_LOCATION_POLICY =
      new Builder(Name.USER_BLOCK_WRITE_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.block.policy.LocalFirstPolicy")
          .setDescription("The default location policy for choosing workers for writing a "
              + "file's blocks.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES =
      new Builder(Name.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES)
          .setDefaultValue("0MB")
          .setDescription("The portion of space reserved in a worker when using the "
              + "LocalFirstAvoidEvictionPolicy class as block location policy.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED =
      new Builder(Name.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED)
          .setDefaultValue(true)
          .setDescription("If this is enabled, cache data asynchronously.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_ASYNC_WRITE_THREADS =
      new Builder(Name.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS)
          .setDefaultValue(16)
          .setDescription("Number of threads to asynchronously cache data.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_ENABLED =
      new Builder(Name.USER_CLIENT_CACHE_ENABLED)
          .setDefaultValue(false)
          .setDescription("If this is enabled, data will be cached on Alluxio client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_EVICTOR_CLASS =
      new Builder(Name.USER_CLIENT_CACHE_EVICTOR_CLASS)
          .setDefaultValue("alluxio.client.file.cache.evictor.LRUCacheEvictor")
          .setDescription("The strategy that client uses to evict local cached pages when running "
              + "out of space. Currently the only valid option provided is "
              + "`alluxio.client.file.cache.evictor.LRUCacheEvictor`.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_EVICTOR_LFU_LOGBASE =
      new Builder(Name.USER_CLIENT_CACHE_EVICTOR_LFU_LOGBASE)
          .setDefaultValue(2.0)
          .setDescription("The log base for client cache LFU evictor bucket index.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_DIR =
      new Builder(Name.USER_CLIENT_CACHE_DIR)
          .setDefaultValue("/tmp/alluxio_cache")
          .setDescription("The directory where client-side cache is stored.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_STORE_TYPE =
      new Builder(Name.USER_CLIENT_CACHE_STORE_TYPE)
          .setDefaultValue("LOCAL")
          .setDescription("The type of page store to use for client-side cache. Can be either "
              + "`LOCAL` or `ROCKS`. The `LOCAL` page store stores all pages in a directory, "
              + "the `ROCKS` page store utilizes rocksDB to persist the data.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_LOCAL_STORE_FILE_BUCKETS =
      new Builder(Name.USER_CLIENT_CACHE_LOCAL_STORE_FILE_BUCKETS)
          .setDefaultValue("1000")
          .setDescription("The number of file buckets for the local page store of the client-side "
              + "cache. It is recommended to set this to a high value if the number of unique "
              + "files is expected to be high (# files / file buckets <= 100,000).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_SIZE =
      new Builder(Name.USER_CLIENT_CACHE_SIZE)
          .setDefaultValue("512MB")
          .setDescription("The maximum size of the client-side cache.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CLIENT_CACHE_PAGE_SIZE =
      new Builder(Name.USER_CLIENT_CACHE_PAGE_SIZE)
          .setDefaultValue("1MB")
          .setDescription("Size of each page in client-side cache.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FILE_WRITE_TYPE_DEFAULT =
      new Builder(Name.USER_FILE_WRITE_TYPE_DEFAULT)
          .setDefaultValue("ASYNC_THROUGH")
      .setDescription(
          String.format("Default write type when creating Alluxio files. Valid " + "options are "
              + "`MUST_CACHE` (write will only go to Alluxio and must be stored in Alluxio), "
              + "`CACHE_THROUGH` (try to cache, write to UnderFS synchronously), `THROUGH` "
              + "(no cache, write to UnderFS synchronously), `ASYNC_THROUGH` (write to cache, "
              + "write to UnderFS asynchronously, replicated %s times in Alluxio before data is "
              + "persisted.", USER_FILE_REPLICATION_DURABLE))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_HOSTNAME = new Builder(Name.USER_HOSTNAME)
      .setDescription("The hostname to use for an Alluxio client.")
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
  public static final PropertyKey USER_LOCAL_READER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_READER_CHUNK_SIZE_BYTES)
          .setDefaultValue("8MB")
          .setDescription("When a client reads from a local worker, the maximum data chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_LOCAL_WRITER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES)
          .setDefaultValue("64KB")
          .setDescription("When a client writes to a local worker, the maximum data chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_LOGGING_THRESHOLD =
      new Builder(Name.USER_LOGGING_THRESHOLD)
          .setDefaultValue("10s")
          .setDescription("Logging a client RPC when it takes more time than the threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METADATA_CACHE_ENABLED =
      new Builder(Name.USER_METADATA_CACHE_ENABLED)
          .setDefaultValue(false)
          .setDescription("If this is enabled, metadata of paths will be cached. "
              + "The cached metadata will be evicted when it expires after "
              + Name.USER_METADATA_CACHE_EXPIRATION_TIME
              + " or the cache size is over the limit of "
              + Name.USER_METADATA_CACHE_MAX_SIZE + ".")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METADATA_CACHE_MAX_SIZE =
      new Builder(Name.USER_METADATA_CACHE_MAX_SIZE)
          .setDefaultValue(100000)
          .setDescription("Maximum number of paths with cached metadata. Only valid if the "
              + "filesystem is alluxio.client.file.MetadataCachingBaseFileSystem.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METADATA_CACHE_EXPIRATION_TIME =
      new Builder(Name.USER_METADATA_CACHE_EXPIRATION_TIME)
          .setDefaultValue("10min")
          .setDescription("Metadata will expire and be evicted after being cached for this time "
              + "period. Only valid if the filesystem is "
              + "alluxio.client.file.MetadataCachingBaseFileSystem.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METRICS_COLLECTION_ENABLED =
      new Builder(Name.USER_METRICS_COLLECTION_ENABLED)
          .setDefaultValue(false)
          .setDescription("Enable collecting the client-side metrics and heartbeat them to master")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_METRICS_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.USER_METRICS_HEARTBEAT_INTERVAL_MS)
          .setAlias("alluxio.user.metrics.heartbeat.interval.ms")
          .setDefaultValue("10sec")
          .setDescription("The time period of client master heartbeat to "
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
  public static final PropertyKey USER_STREAMING_DATA_TIMEOUT =
      new Builder(Name.USER_STREAMING_DATA_TIMEOUT)
          .setAlias("alluxio.user.network.data.timeout.ms", Name.USER_NETWORK_DATA_TIMEOUT)
          .setDefaultValue("30sec")
          .setDescription("The maximum time for an Alluxio client to wait for a data response "
              + "(e.g. block reads and block writes) from Alluxio worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_READER_BUFFER_SIZE_MESSAGES =
      new Builder(Name.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES)
          .setAlias(Name.USER_NETWORK_READER_BUFFER_SIZE_MESSAGES)
          .setDefaultValue(16)
          .setDescription("When a client reads from a remote worker, the maximum number of "
              + "messages to buffer by the client. A message can be either a command response, "
              + "a data chunk, or a gRPC stream event such as complete or error.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_READER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES)
          .setAlias(Name.USER_NETWORK_READER_CHUNK_SIZE_BYTES)
          .setDefaultValue("1MB")
          .setDescription("When a client reads from a remote worker, the maximum chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_WRITER_BUFFER_SIZE_MESSAGES =
      new Builder(Name.USER_STREAMING_WRITER_BUFFER_SIZE_MESSAGES)
          .setAlias(Name.USER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES)
          .setDefaultValue(16)
          .setDescription("When a client writes to a remote worker, the maximum number of messages "
              + "to buffer by the client. A message can be either a command response, a data "
              + "chunk, or a gRPC stream event such as complete or error.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_WRITER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_STREAMING_WRITER_CHUNK_SIZE_BYTES)
          .setAlias(Name.USER_NETWORK_WRITER_CHUNK_SIZE_BYTES)
          .setDefaultValue("1MB")
          .setDescription("When a client writes to a remote worker, the maximum chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_WRITER_CLOSE_TIMEOUT =
      new Builder(Name.USER_STREAMING_WRITER_CLOSE_TIMEOUT)
          .setAlias("alluxio.user.network.writer.close.timeout.ms",
              Name.USER_NETWORK_WRITER_CLOSE_TIMEOUT)
          .setDefaultValue("30min")
          .setDescription("The timeout to close a writer client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_WRITER_FLUSH_TIMEOUT =
      new Builder(Name.USER_STREAMING_WRITER_FLUSH_TIMEOUT)
          .setAlias(Name.USER_NETWORK_WRITER_FLUSH_TIMEOUT)
          .setDefaultValue("30min")
          .setDescription("The timeout to wait for flush to finish in a data writer.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_STREAMING_ZEROCOPY_ENABLED =
      new Builder(Name.USER_STREAMING_ZEROCOPY_ENABLED)
          .setAlias(Name.USER_NETWORK_ZEROCOPY_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether zero copy is enabled on client when processing data streams.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_DATA_TIMEOUT} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_DATA_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_DATA_TIMEOUT)
          .setAlias("alluxio.user.network.data.timeout.ms")
          .setDescription("The maximum time for an Alluxio client to wait for a data response "
              + "(e.g. block reads and block writes) from Alluxio worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_READER_BUFFER_SIZE_MESSAGES} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_READER_BUFFER_SIZE_MESSAGES =
      new Builder(Name.USER_NETWORK_READER_BUFFER_SIZE_MESSAGES)
          .setDescription("When a client reads from a remote worker, the maximum number of "
              + "messages to buffer by the client. A message can be either a command response, "
              + "a data chunk, or a gRPC stream event such as complete or error.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_READER_CHUNK_SIZE_BYTES} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_READER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_READER_CHUNK_SIZE_BYTES)
          .setDescription("When a client reads from a remote worker, the maximum chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_WRITER_BUFFER_SIZE_MESSAGES} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES =
      new Builder(Name.USER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES)
          .setDescription("When a client writes to a remote worker, the maximum number of messages "
              + "to buffer by the client. A message can be either a command response, a data "
              + "chunk, or a gRPC stream event such as complete or error.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_WRITER_CHUNK_SIZE_BYTES} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_WRITER_CHUNK_SIZE_BYTES =
      new Builder(Name.USER_NETWORK_WRITER_CHUNK_SIZE_BYTES)
          .setDescription("When a client writes to a remote worker, the maximum chunk size.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_WRITER_CLOSE_TIMEOUT} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_WRITER_CLOSE_TIMEOUT_MS =
      new Builder(Name.USER_NETWORK_WRITER_CLOSE_TIMEOUT)
          .setAlias("alluxio.user.network.writer.close.timeout.ms")
          .setDescription("The timeout to close a writer client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_WRITER_FLUSH_TIMEOUT} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_WRITER_FLUSH_TIMEOUT =
      new Builder(Name.USER_NETWORK_WRITER_FLUSH_TIMEOUT)
          .setDescription("The timeout to wait for flush to finish in a data writer.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_STREAMING_ZEROCOPY_ENABLED} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_ZEROCOPY_ENABLED =
      new Builder(Name.USER_NETWORK_ZEROCOPY_ENABLED)
          .setDescription("Whether zero copy is enabled on client when processing data streams.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_FLOWCONTROL_WINDOW} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_FLOWCONTROL_WINDOW =
      new Builder(Name.USER_NETWORK_FLOWCONTROL_WINDOW)
          .setDescription("The HTTP2 flow control window used by user gRPC connections. Larger "
              + "value will allow more data to be buffered but will use more memory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_KEEPALIVE_TIME} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_KEEPALIVE_TIME =
      new Builder(Name.USER_NETWORK_KEEPALIVE_TIME)
          .setDescription("The amount of time for a gRPC client (for block reads and block writes) "
              + "to wait for a response before pinging the server to see if it is still alive.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_KEEPALIVE_TIMEOUT} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_KEEPALIVE_TIMEOUT =
      new Builder(Name.USER_NETWORK_KEEPALIVE_TIMEOUT)
          .setDescription("The maximum time for a gRPC client (for block reads and block writes) "
              + "to wait for a keepalive response before closing the connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_MAX_INBOUND_MESSAGE_SIZE} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE)
          .setDescription("The max inbound message size used by user gRPC connections.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_FLOWCONTROL_WINDOW} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_NETTY_CHANNEL =
      new Builder(Name.USER_NETWORK_NETTY_CHANNEL)
          .setDescription("Type of netty channels. If EPOLL is not available, this will "
              + "automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  /**
   * @deprecated use {@link #USER_NETWORK_STREAMING_NETTY_WORKER_THREADS} instead
   */
  @Deprecated
  public static final PropertyKey USER_NETWORK_NETTY_WORKER_THREADS =
      new Builder(Name.USER_NETWORK_NETTY_WORKER_THREADS)
          .setDescription("How many threads to use for remote block worker client to read "
              + "from remote block workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_FLOWCONTROL_WINDOW =
      new Builder(Name.USER_NETWORK_RPC_FLOWCONTROL_WINDOW)
          .setDefaultValue("2MB")
          .setDescription("The HTTP2 flow control window used by user rpc connections. "
              + "Larger value will allow more data to be buffered but will use more memory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_KEEPALIVE_TIME =
      new Builder(Name.USER_NETWORK_RPC_KEEPALIVE_TIME)
          .setDefaultValue(Long.MAX_VALUE)
          .setDescription("The amount of time for a rpc client "
              + "to wait for a response before pinging the server to see if it is still alive.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_KEEPALIVE_TIMEOUT =
      new Builder(Name.USER_NETWORK_RPC_KEEPALIVE_TIMEOUT)
          .setDefaultValue("30sec")
          .setDescription("The maximum time for a rpc client "
              + "to wait for a keepalive response before closing the connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.USER_NETWORK_RPC_MAX_INBOUND_MESSAGE_SIZE)
          .setDefaultValue("100MB")
          .setDescription("The max inbound message size used by user rpc connections.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_NETTY_CHANNEL =
      new Builder(Name.USER_NETWORK_RPC_NETTY_CHANNEL)
          .setDescription("Type of netty channels used by rpc connections. "
              + "If EPOLL is not available, this will automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setDefaultValue("EPOLL")
          .build();
  public static final PropertyKey USER_NETWORK_RPC_NETTY_WORKER_THREADS =
      new Builder(Name.USER_NETWORK_RPC_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .setDescription("How many threads to use for rpc client to read "
              + "from remote workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_RPC_MAX_CONNECTIONS =
      new Builder(Name.USER_NETWORK_RPC_MAX_CONNECTIONS)
          .setDefaultValue(1)
          .setDescription(
              "The maximum number of physical connections to be "
              + "used per target host.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_FLOWCONTROL_WINDOW =
      new Builder(Name.USER_NETWORK_STREAMING_FLOWCONTROL_WINDOW)
          .setAlias(Name.USER_NETWORK_FLOWCONTROL_WINDOW)
          .setDefaultValue("2MB")
          .setDescription("The HTTP2 flow control window used by user streaming connections. "
              + "Larger value will allow more data to be buffered but will use more memory.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_KEEPALIVE_TIME =
      new Builder(Name.USER_NETWORK_STREAMING_KEEPALIVE_TIME)
          .setAlias(Name.USER_NETWORK_KEEPALIVE_TIME)
          .setDefaultValue(Long.MAX_VALUE)
          .setDescription("The amount of time for a streaming client "
              + "to wait for a response before pinging the server to see if it is still alive.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_KEEPALIVE_TIMEOUT =
      new Builder(Name.USER_NETWORK_STREAMING_KEEPALIVE_TIMEOUT)
          .setAlias(Name.USER_NETWORK_KEEPALIVE_TIMEOUT)
          .setDefaultValue("30sec")
          .setDescription("The maximum time for a streaming client "
              + "to wait for a keepalive response before closing the connection.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_MAX_INBOUND_MESSAGE_SIZE =
      new Builder(Name.USER_NETWORK_STREAMING_MAX_INBOUND_MESSAGE_SIZE)
          .setAlias(Name.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE)
          .setDefaultValue("100MB")
          .setDescription("The max inbound message size used by user streaming connections.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_NETTY_CHANNEL =
      new Builder(Name.USER_NETWORK_STREAMING_NETTY_CHANNEL)
          .setAlias(Name.USER_NETWORK_NETTY_CHANNEL)
          .setDescription("Type of netty channels used by streaming connections. "
              + "If EPOLL is not available, this will automatically fall back to NIO.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .setDefaultValue("EPOLL")
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_NETTY_WORKER_THREADS =
      new Builder(Name.USER_NETWORK_STREAMING_NETTY_WORKER_THREADS)
          .setAlias(Name.USER_NETWORK_NETTY_WORKER_THREADS)
          .setDefaultValue(0)
          .setDescription("How many threads to use for streaming client to read "
              + "from remote workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_NETWORK_STREAMING_MAX_CONNECTIONS =
      new Builder(Name.USER_NETWORK_STREAMING_MAX_CONNECTIONS)
          .setDefaultValue(64)
          .setDescription(
              "The maximum number of physical connections to be "
              + "used per target host.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_RPC_RETRY_BASE_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_BASE_SLEEP_MS)
          .setAlias("alluxio.user.rpc.retry.base.sleep.ms")
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
  public static final PropertyKey USER_RPC_RETRY_MAX_SLEEP_MS =
      new Builder(Name.USER_RPC_RETRY_MAX_SLEEP_MS)
          .setAlias("alluxio.user.rpc.retry.max.sleep.ms")
          .setDefaultValue("3sec")
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the maximum wait time "
              + "in the backoff.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED =
      new Builder(Name.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to return all workers as block location if ufs block locations "
              + "are not co-located with any Alluxio workers or is empty.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_UFS_BLOCK_READ_LOCATION_POLICY =
      new Builder(Name.USER_UFS_BLOCK_READ_LOCATION_POLICY)
          .setDefaultValue("alluxio.client.block.policy.LocalFirstPolicy")
          .setDescription(String.format("When an Alluxio client reads a file from the UFS, it "
              + "delegates the read to an Alluxio worker. The client uses this policy to choose "
              + "which worker to read through. Built-in choices: %s.", Arrays.asList(
              javadocLink("alluxio.client.block.policy.DeterministicHashPolicy"),
              javadocLink("alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy"),
              javadocLink("alluxio.client.block.policy.LocalFirstPolicy"),
              javadocLink("alluxio.client.block.policy.MostAvailableFirstPolicy"),
              javadocLink("alluxio.client.block.policy.RoundRobinPolicy"),
              javadocLink("alluxio.client.block.policy.SpecificHostPolicy"))))
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
  public static final PropertyKey USER_UPDATE_FILE_ACCESSTIME_DISABLED =
      new Builder(Name.USER_UPDATE_FILE_ACCESSTIME_DISABLED)
          .setDefaultValue(false)
          .setIsHidden(true)
          .setDescription("(Experimental) If this is enabled, the clients doesn't update file "
              + "access time which may cause issues for some applications.")
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
  public static final PropertyKey USER_SHORT_CIRCUIT_PREFERRED =
      new Builder(Name.USER_SHORT_CIRCUIT_PREFERRED)
          .setDefaultValue(false)
          .setDescription("When short circuit and domain socket both enabled, "
              + "prefer to use short circuit.")
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
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_DEBUG_ENABLED =
      new Builder(Name.FUSE_DEBUG_ENABLED)
          .setDefaultValue(false)
          .setDescription("Run FUSE in debug mode, and have the fuse process log every FS request.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_FS_NAME =
      new Builder(Name.FUSE_FS_NAME)
          .setDefaultValue("alluxio-fuse")
          .setDescription("The FUSE file system name.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_JNIFUSE_ENABLED =
      new Builder(Name.FUSE_JNIFUSE_ENABLED)
          .setDefaultValue(false)
          .setDescription("(Experimental) Use experimental JNIFUSE library for better performance.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_MAXCACHE_BYTES =
      new Builder(Name.FUSE_MAXCACHE_BYTES)
          .setDefaultValue("1MB")
          .setDescription("(Experimental) Maximum cache size of AlluxioJniFuse.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_LOGGING_THRESHOLD =
      new Builder(Name.FUSE_LOGGING_THRESHOLD)
          .setDefaultValue("10s")
          .setDescription("Logging a FUSE API call when it takes more time than the threshold.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_MAXWRITE_BYTES =
      new Builder(Name.FUSE_MAXWRITE_BYTES)
          .setDefaultValue("128KB")
          .setDescription("Maximum granularity of write operations, capped by the kernel to 128KB "
              + "max (as of Linux 3.16.0).")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey FUSE_USER_GROUP_TRANSLATION_ENABLED =
      new Builder(Name.FUSE_USER_GROUP_TRANSLATION_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to translate Alluxio users and groups "
              + "into Unix users and groups when exposing Alluxio files through the FUSE API. "
              + "When this property is set to false, the user and group for all FUSE files "
              + "will match the user who started the alluxio-fuse process.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.CLIENT)
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey SECURITY_AUTHENTICATION_TYPE =
      new Builder(Name.SECURITY_AUTHENTICATION_TYPE)
          .setDefaultValue("SIMPLE")
          .setDescription("The authentication mode. Currently three modes are supported: "
              + "NOSASL, SIMPLE, CUSTOM. The default value SIMPLE indicates that a simple "
              + "authentication is enabled. Server trusts whoever the client claims to be.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED)
          .setDefaultValue(true)
          .setDescription("Whether to enable access control based on file permission.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
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
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS =
      new Builder(Name.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS)
          .setAlias("alluxio.security.group.mapping.cache.timeout.ms")
          .setDefaultValue("1min")
          .setDescription("Time for cached group mapping to expire.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
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
          .setScope(Scope.ALL)
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
  public static final PropertyKey AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD =
      new Builder(Name.AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD)
          .setDefaultValue("3day")
          .setDescription("Interval for which client channels that have been inactive "
                  + "will be regarded as unauthenticated. Such channels will reauthenticate with "
                  + "their target master upon being used for new RPCs.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.ALL)
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
              "http://downloads.alluxio.io/downloads/files/${%s}/alluxio-${%s}-bin.tar.gz",
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
  // Assumes that HDFS is the UFS and version is 2.7
  // TODO(ns) Fix default value to handle other UFS types
  public static final PropertyKey UNDERFS_VERSION =
      new Builder(Name.UNDERFS_VERSION)
          .setDefaultValue("2.7")
          .setIsHidden(true)
          .build();

  //
  // Job service
  //
  public static final PropertyKey JOB_MASTER_CLIENT_THREADS =
      new Builder(Name.JOB_MASTER_CLIENT_THREADS)
          .setDescription("The number of threads the Alluxio master uses to make requests to the "
              + "job master.")
          .setDefaultValue(1024)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_FINISHED_JOB_PURGE_COUNT =
      new Builder(Name.JOB_MASTER_FINISHED_JOB_PURGE_COUNT)
          .setDescription("The maximum amount of jobs to purge at any single time when the job "
              + "master reaches its maximum capacity. It is recommended to set this value when "
              + "setting the capacity of the job master to a large ( > 10M) value. Default is -1 "
              + "denoting an unlimited value")
          .setDefaultValue("-1")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_FINISHED_JOB_RETENTION_TIME =
      new Builder(Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME)
          .setDescription("The length of time the Alluxio Job Master should save information about "
              + "completed jobs before they are discarded.")
          .setDefaultValue("300sec")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_JOB_CAPACITY =
      new Builder(Name.JOB_MASTER_JOB_CAPACITY)
          .setDescription("The total possible number of available job statuses in the job master. "
              + "This value includes running and finished jobs which are have completed within "
              + Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME + ".")
          .setDefaultValue(100000)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_WORKER_HEARTBEAT_INTERVAL =
      new Builder(Name.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL)
          .setDescription("The amount of time that the Alluxio job worker should wait in between "
              + "heartbeats to the Job Master.")
          .setDefaultValue("1sec")
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_MASTER_WORKER_TIMEOUT =
      new Builder(Name.JOB_MASTER_WORKER_TIMEOUT)
          .setDescription("The time period after which the job master will mark a worker as lost "
              + "without a subsequent heartbeat.")
          .setDefaultValue("60sec")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_BIND_HOST =
      new Builder(Name.JOB_MASTER_BIND_HOST)
          .setDescription("The host that the Alluxio job master will bind to.")
          .setDefaultValue("0.0.0.0")
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey JOB_MASTER_HOSTNAME =
      new Builder(Name.JOB_MASTER_HOSTNAME)
          .setDescription("The hostname of the Alluxio job master.")
          .setDefaultValue(String.format("${%s}", Name.MASTER_HOSTNAME))
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey JOB_MASTER_LOST_WORKER_INTERVAL =
      new Builder(Name.JOB_MASTER_LOST_WORKER_INTERVAL)
          .setDescription("The time interval the job master waits between checks for lost workers.")
          .setDefaultValue("1sec")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_RPC_PORT =
      new Builder(Name.JOB_MASTER_RPC_PORT)
          .setDescription("The port for Alluxio job master's RPC service.")
          .setDefaultValue(20001)
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey JOB_MASTER_WEB_BIND_HOST =
      new Builder(Name.JOB_MASTER_WEB_BIND_HOST)
          .setDescription("The host that the job master web server binds to.")
          .setDefaultValue("0.0.0.0")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_WEB_HOSTNAME =
      new Builder(Name.JOB_MASTER_WEB_HOSTNAME)
          .setDescription("The hostname of the job master web server.")
          .setDefaultValue("${alluxio.job.master.hostname}")
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_MASTER_WEB_PORT =
      new Builder(Name.JOB_MASTER_WEB_PORT)
          .setDescription("The port the job master web server uses.")
          .setDefaultValue(20002)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey JOB_WORKER_BIND_HOST =
      new Builder(Name.JOB_WORKER_BIND_HOST)
          .setDescription("The host that the Alluxio job worker will bind to.")
          .setDefaultValue("0.0.0.0")
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_DATA_PORT =
      new Builder(Name.JOB_WORKER_DATA_PORT)
          .setDescription("The port the Alluxio Job worker uses to send data.")
          .setDefaultValue(30002)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_HOSTNAME =
      new Builder(Name.JOB_WORKER_HOSTNAME)
          .setDescription("The hostname of the Alluxio job worker.")
          .setDefaultValue(String.format("${%s}", Name.WORKER_HOSTNAME))
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_RPC_PORT =
      new Builder(Name.JOB_WORKER_RPC_PORT)
          .setDescription("The port for Alluxio job worker's RPC service.")
          .setDefaultValue(30001)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_THREADPOOL_SIZE =
      new Builder(Name.JOB_WORKER_THREADPOOL_SIZE)
          .setDescription("Number of threads in the thread pool for job worker. "
              + "This may be adjusted to a lower value to alleviate resource "
              + "saturation on the job worker nodes (CPU + IO).")
          .setDefaultValue(10)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_THROTTLING =
      new Builder(Name.JOB_WORKER_THROTTLING)
          .setDescription("Whether the job worker should throttle itself based on whether the"
              + "resources are saturated.")
          .setScope(Scope.WORKER)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey JOB_WORKER_WEB_BIND_HOST =
      new Builder(Name.JOB_WORKER_WEB_BIND_HOST)
          .setDescription("The host the job worker web server binds to.")
          .setDefaultValue("0.0.0.0")
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_WORKER_WEB_PORT =
      new Builder(Name.JOB_WORKER_WEB_PORT)
          .setDescription("The port the Alluxio job worker web server uses.")
          .setDefaultValue(30003)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey JOB_MASTER_RPC_ADDRESSES =
      new Builder(Name.JOB_MASTER_RPC_ADDRESSES)
          .setDescription(String.format("The list of RPC addresses to use for the job service "
                  + "configured in non-zookeeper HA mode. If this property is not specifically "
                  + "defined, it will first fall back to using %s, replacing those address "
                  + "ports with the port defined by %s. Otherwise the addresses are inherited from "
                  + "%s using the port defined in %s",
              Name.MASTER_RPC_ADDRESSES, Name.JOB_MASTER_RPC_PORT,
              Name.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, Name.JOB_MASTER_RPC_PORT))
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES =
      new Builder(Name.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)
          .setDescription(String.format("A comma-separated list of journal addresses for all job "
              + "masters in the cluster. The format is 'hostname1:port1,hostname2:port2,...'. "
              + "Defaults to the journal addresses set for the Alluxio masters (%s), but with the "
              + "job master embedded journal port.", Name.MASTER_EMBEDDED_JOURNAL_ADDRESSES))
          .setScope(Scope.ALL)
          .build();
  public static final PropertyKey JOB_MASTER_EMBEDDED_JOURNAL_PORT =
      new Builder(Name.JOB_MASTER_EMBEDDED_JOURNAL_PORT)
          .setDescription(
              "The port to use for embedded journal communication with other job masters.")
          .setDefaultValue(20003)
          .setScope(Scope.ALL)
          .build();

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

  //
  // Table service properties
  //
  public static final PropertyKey TABLE_ENABLED =
      new Builder(Name.TABLE_ENABLED)
          .setDefaultValue(true)
          .setDescription("(Experimental) Enables the table service.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey TABLE_CATALOG_PATH =
      new Builder(Name.TABLE_CATALOG_PATH)
          .setDefaultValue("/catalog")
          .setDescription("The Alluxio file path for the table catalog metadata.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey TABLE_CATALOG_UDB_SYNC_TIMEOUT =
      new Builder(Name.TABLE_CATALOG_UDB_SYNC_TIMEOUT)
          .setDefaultValue("1h")
          .setDescription("The timeout period for a db sync to finish in the catalog. If a sync"
              + "takes longer than this timeout, the sync will be terminated.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey TABLE_TRANSFORM_MANAGER_JOB_MONITOR_INTERVAL =
      new Builder(Name.TABLE_TRANSFORM_MANAGER_JOB_MONITOR_INTERVAL)
          .setDefaultValue(10 * Constants.SECOND_MS)
          .setDescription("Job monitor is a heartbeat thread in the transform manager, "
              + "this is the time interval in milliseconds the job monitor heartbeat is run to "
              + "check the status of the transformation jobs and update table and partition "
              + "locations after transformation.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME =
      new Builder(Name.TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME)
          .setDefaultValue("300sec")
          .setDescription("The length of time the Alluxio Table Master should keep information "
              + "about finished transformation jobs before they are discarded.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();

  /**
   * @deprecated This key is used for testing. It is always deprecated.
   */
  @Deprecated(message = "This key is used only for testing. It is always deprecated")
  public static final PropertyKey TEST_DEPRECATED_KEY =
      new Builder("alluxio.test.deprecated.key")
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
    public static final String LOGGER_TYPE = "alluxio.logger.type";
    public static final String LOGS_DIR = "alluxio.logs.dir";
    public static final String METRICS_CONF_FILE = "alluxio.metrics.conf.file";
    public static final String METRICS_CONTEXT_SHUTDOWN_TIMEOUT =
        "alluxio.metrics.context.shutdown.timeout";
    public static final String NETWORK_CONNECTION_AUTH_TIMEOUT =
        "alluxio.network.connection.auth.timeout";
    public static final String NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT =
        "alluxio.network.connection.health.check.timeout";
    public static final String NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT =
        "alluxio.network.connection.server.shutdown.timeout";
    public static final String NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT =
        "alluxio.network.connection.shutdown.graceful.timeout";
    public static final String NETWORK_CONNECTION_SHUTDOWN_TIMEOUT =
        "alluxio.network.connection.shutdown.timeout";
    public static final String NETWORK_HOST_RESOLUTION_TIMEOUT_MS =
        "alluxio.network.host.resolution.timeout";
    public static final String SITE_CONF_DIR = "alluxio.site.conf.dir";
    public static final String TEST_MODE = "alluxio.test.mode";
    public static final String TMP_DIRS = "alluxio.tmp.dirs";
    public static final String USER_LOGS_DIR = "alluxio.user.logs.dir";
    public static final String VERSION = "alluxio.version";
    public static final String WEB_FILE_INFO_ENABLED = "alluxio.web.file.info.enabled";
    public static final String WEB_RESOURCES = "alluxio.web.resources";
    public static final String WEB_THREADS = "alluxio.web.threads";
    public static final String WEB_CORS_ENABLED = "alluxio.web.cors.enabled";
    public static final String WEB_REFRESH_INTERVAL = "alluxio.web.refresh.interval";
    public static final String WEB_UI_ENABLED = "alluxio.web.ui.enabled";
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
    public static final String ZOOKEEPER_AUTH_ENABLED = "alluxio.zookeeper.auth.enabled";
    public static final String ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY =
        "alluxio.zookeeper.leader.connection.error.policy";
    //
    // UFS related properties
    //
    public static final String UNDERFS_ALLOW_SET_OWNER_FAILURE =
        "alluxio.underfs.allow.set.owner.failure";
    public static final String UNDERFS_CLEANUP_ENABLED = "alluxio.underfs.cleanup.enabled";
    public static final String UNDERFS_CLEANUP_INTERVAL = "alluxio.underfs.cleanup.interval";
    public static final String UNDERFS_EVENTUAL_CONSISTENCY_RETRY_BASE_SLEEP_MS =
        "alluxio.underfs.eventual.consistency.retry.base.sleep";
    public static final String UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_NUM =
        "alluxio.underfs.eventual.consistency.retry.max.num";
    public static final String UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_SLEEP_MS =
        "alluxio.underfs.eventual.consistency.retry.max.sleep";
    public static final String UNDERFS_LISTING_LENGTH = "alluxio.underfs.listing.length";
    public static final String UNDERFS_GCS_DEFAULT_MODE = "alluxio.underfs.gcs.default.mode";
    public static final String UNDERFS_GCS_DIRECTORY_SUFFIX =
        "alluxio.underfs.gcs.directory.suffix";
    public static final String UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING =
        "alluxio.underfs.gcs.owner.id.to.username.mapping";
    public static final String UNDERFS_HDFS_CONFIGURATION = "alluxio.underfs.hdfs.configuration";
    public static final String UNDERFS_HDFS_IMPL = "alluxio.underfs.hdfs.impl";
    public static final String UNDERFS_HDFS_PREFIXES = "alluxio.underfs.hdfs.prefixes";
    public static final String UNDERFS_HDFS_REMOTE = "alluxio.underfs.hdfs.remote";
    public static final String UNDERFS_WEB_HEADER_LAST_MODIFIED =
        "alluxio.underfs.web.header.last.modified";
    public static final String UNDERFS_WEB_CONNECTION_TIMEOUT =
        "alluxio.underfs.web.connnection.timeout";
    public static final String UNDERFS_WEB_PARENT_NAMES = "alluxio.underfs.web.parent.names";
    public static final String UNDERFS_WEB_TITLES = "alluxio.underfs.web.titles";
    public static final String UNDERFS_VERSION = "alluxio.underfs.version";
    public static final String UNDERFS_OBJECT_STORE_BREADCRUMBS_ENABLED =
        "alluxio.underfs.object.store.breadcrumbs.enabled";
    public static final String UNDERFS_OBJECT_STORE_SERVICE_THREADS =
        "alluxio.underfs.object.store.service.threads";
    public static final String UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY =
        "alluxio.underfs.object.store.mount.shared.publicly";
    public static final String UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE =
        "alluxio.underfs.object.store.multi.range.chunk.size";
    public static final String UNDERFS_OSS_CONNECT_MAX = "alluxio.underfs.oss.connection.max";
    public static final String UNDERFS_OSS_CONNECT_TIMEOUT =
        "alluxio.underfs.oss.connection.timeout";
    public static final String UNDERFS_OSS_CONNECT_TTL = "alluxio.underfs.oss.connection.ttl";
    public static final String UNDERFS_OSS_SOCKET_TIMEOUT = "alluxio.underfs.oss.socket.timeout";
    public static final String UNDERFS_S3_BULK_DELETE_ENABLED =
        "alluxio.underfs.s3.bulk.delete.enabled";
    public static final String UNDERFS_S3_DEFAULT_MODE = "alluxio.underfs.s3.default.mode";
    public static final String UNDERFS_S3_DIRECTORY_SUFFIX =
        "alluxio.underfs.s3.directory.suffix";
    public static final String UNDERFS_S3_INHERIT_ACL = "alluxio.underfs.s3.inherit.acl";
    public static final String UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE =
        "alluxio.underfs.s3.intermediate.upload.clean.age";
    public static final String UNDERFS_S3_LIST_OBJECTS_V1 =
        "alluxio.underfs.s3.list.objects.v1";
    public static final String UNDERFS_S3_MAX_ERROR_RETRY =
        "alluxio.underfs.s3.max.error.retry";
    public static final String UNDERFS_S3_REQUEST_TIMEOUT =
        "alluxio.underfs.s3.request.timeout";
    public static final String UNDERFS_S3_SECURE_HTTP_ENABLED =
        "alluxio.underfs.s3.secure.http.enabled";
    public static final String UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED =
        "alluxio.underfs.s3.server.side.encryption.enabled";
    public static final String UNDERFS_S3_SIGNER_ALGORITHM =
        "alluxio.underfs.s3.signer.algorithm";
    public static final String UNDERFS_S3_SOCKET_TIMEOUT =
        "alluxio.underfs.s3.socket.timeout";
    public static final String UNDERFS_S3_STREAMING_UPLOAD_ENABLED =
        "alluxio.underfs.s3.streaming.upload.enabled";
    public static final String UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE =
        "alluxio.underfs.s3.streaming.upload.partition.size";
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
    public static final String KODO_ENDPOINT = "alluxio.underfs.kodo.endpoint";
    public static final String KODO_DOWNLOAD_HOST = "alluxio.underfs.kodo.downloadhost";
    public static final String UNDERFS_KODO_CONNECT_TIMEOUT =
        "alluxio.underfs.kodo.connect.timeout";
    public static final String UNDERFS_KODO_REQUESTS_MAX = "alluxio.underfs.kodo.requests.max";

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
    public static final String SWIFT_AUTH_METHOD_KEY = "fs.swift.auth.method";
    public static final String SWIFT_AUTH_URL_KEY = "fs.swift.auth.url";
    public static final String SWIFT_PASSWORD_KEY = "fs.swift.password";
    public static final String SWIFT_REGION_KEY = "fs.swift.region";
    public static final String SWIFT_SIMULATION = "fs.swift.simulation";
    public static final String SWIFT_TENANT_KEY = "fs.swift.tenant";
    public static final String SWIFT_USER_KEY = "fs.swift.user";
    public static final String KODO_ACCESS_KEY = "fs.kodo.accesskey";
    public static final String KODO_SECRET_KEY = "fs.kodo.secretkey";

    //
    // Master related properties
    //
    public static final String MASTER_AUDIT_LOGGING_ENABLED =
        "alluxio.master.audit.logging.enabled";
    public static final String MASTER_AUDIT_LOGGING_QUEUE_CAPACITY =
        "alluxio.master.audit.logging.queue.capacity";
    public static final String MASTER_BACKUP_DIRECTORY =
        "alluxio.master.backup.directory";
    public static final String MASTER_BACKUP_ENTRY_BUFFER_COUNT =
        "alluxio.master.backup.entry.buffer.count";
    public static final String MASTER_BACKUP_DELEGATION_ENABLED =
        "alluxio.master.backup.delegation.enabled";
    public static final String MASTER_BACKUP_TRANSPORT_TIMEOUT =
        "alluxio.master.backup.transport.timeout";
    public static final String MASTER_BACKUP_HEARTBEAT_INTERVAL =
        "alluxio.master.backup.heartbeat.interval";
    public static final String MASTER_BACKUP_CONNECT_INTERVAL_MIN =
        "alluxio.master.backup.connect.interval.min";
    public static final String MASTER_BACKUP_CONNECT_INTERVAL_MAX =
        "alluxio.master.backup.connect.interval.max";
    public static final String MASTER_BACKUP_ABANDON_TIMEOUT =
        "alluxio.master.backup.abandon.timeout";
    public static final String MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION =
        "alluxio.master.backup.state.lock.exclusive.duration";
    public static final String MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED =
        "alluxio.master.backup.state.lock.interrupt.cycle.enabled";
    public static final String MASTER_BACKUP_STATE_LOCK_FORCED_DURATION =
        "alluxio.master.backup.state.lock.forced.duration";
    public static final String MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL =
        "alluxio.master.backup.state.lock.interrupt.cycle.interval";
    public static final String MASTER_SHELL_BACKUP_STATE_LOCK_GRACE_MODE =
        "alluxio.master.shell.backup.state.lock.grace.mode";
    public static final String MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION =
        "alluxio.master.shell.backup.state.lock.try.duration";
    public static final String MASTER_SHELL_BACKUP_STATE_LOCK_SLEEP_DURATION =
        "alluxio.master.shell.backup.state.lock.sleep.duration";
    public static final String MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT =
        "alluxio.master.shell.backup.state.lock.timeout";
    public static final String MASTER_DAILY_BACKUP_ENABLED =
        "alluxio.master.daily.backup.enabled";
    public static final String MASTER_DAILY_BACKUP_FILES_RETAINED =
        "alluxio.master.daily.backup.files.retained";
    public static final String MASTER_DAILY_BACKUP_TIME =
        "alluxio.master.daily.backup.time";
    public static final String MASTER_DAILY_BACKUP_STATE_LOCK_GRACE_MODE =
        "alluxio.master.daily.backup.state.lock.grace.mode";
    public static final String MASTER_DAILY_BACKUP_STATE_LOCK_TRY_DURATION =
        "alluxio.master.daily.backup.state.lock.try.duration";
    public static final String MASTER_DAILY_BACKUP_STATE_LOCK_SLEEP_DURATION =
        "alluxio.master.daily.backup.state.lock.sleep.duration";
    public static final String MASTER_DAILY_BACKUP_STATE_LOCK_TIMEOUT =
        "alluxio.master.daily.backup.state.lock.timeout";
    public static final String MASTER_BIND_HOST = "alluxio.master.bind.host";
    public static final String MASTER_CLUSTER_METRICS_UPDATE_INTERVAL =
        "alluxio.master.cluster.metrics.update.interval";
    public static final String MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL =
        "alluxio.master.file.access.time.journal.flush.interval";
    public static final String MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION =
        "alluxio.master.file.access.time.update.precision";
    public static final String MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT =
        "alluxio.master.file.access.time.updater.shutdown.timeout";
    public static final String MASTER_FORMAT_FILE_PREFIX = "alluxio.master.format.file.prefix";
    public static final String MASTER_STANDBY_HEARTBEAT_INTERVAL =
        "alluxio.master.standby.heartbeat.interval";
    public static final String MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL =
        "alluxio.master.lost.worker.file.detection.interval";
    public static final String MASTER_HEARTBEAT_TIMEOUT =
        "alluxio.master.heartbeat.timeout";
    public static final String MASTER_HOSTNAME = "alluxio.master.hostname";
    public static final String MASTER_LOCK_POOL_INITSIZE =
        "alluxio.master.lock.pool.initsize";
    public static final String MASTER_LOCK_POOL_LOW_WATERMARK =
        "alluxio.master.lock.pool.low.watermark";
    public static final String MASTER_LOCK_POOL_HIGH_WATERMARK =
        "alluxio.master.lock.pool.high.watermark";
    public static final String MASTER_LOCK_POOL_CONCURRENCY_LEVEL =
        "alluxio.master.lock.pool.concurrency.level";
    public static final String MASTER_JOURNAL_FLUSH_BATCH_TIME_MS =
        "alluxio.master.journal.flush.batch.time";
    public static final String MASTER_JOURNAL_FLUSH_TIMEOUT_MS =
        "alluxio.master.journal.flush.timeout";
    public static final String MASTER_JOURNAL_FLUSH_RETRY_INTERVAL =
        "alluxio.master.journal.retry.interval";
    public static final String MASTER_JOURNAL_FOLDER = "alluxio.master.journal.folder";
    public static final String MASTER_JOURNAL_INIT_FROM_BACKUP =
        "alluxio.master.journal.init.from.backup";
    public static final String MASTER_JOURNAL_TOLERATE_CORRUPTION
        = "alluxio.master.journal.tolerate.corruption";
    public static final String MASTER_JOURNAL_TYPE = "alluxio.master.journal.type";
    public static final String MASTER_JOURNAL_LOG_SIZE_BYTES_MAX =
        "alluxio.master.journal.log.size.bytes.max";
    public static final String MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS =
        "alluxio.master.journal.tailer.shutdown.quiet.wait.time";
    public static final String MASTER_JOURNAL_TAILER_SLEEP_TIME_MS =
        "alluxio.master.journal.tailer.sleep.time";
    public static final String MASTER_RPC_ADDRESSES = "alluxio.master.rpc.addresses";
    public static final String MASTER_EMBEDDED_JOURNAL_PROXY_HOST =
        "alluxio.master.embedded.journal.bind.host";
    public static final String MASTER_EMBEDDED_JOURNAL_ADDRESSES =
        "alluxio.master.embedded.journal.addresses";
    public static final String MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT =
        "alluxio.master.embedded.journal.election.timeout";
    public static final String MASTER_EMBEDDED_JOURNAL_APPENDER_BATCH_SIZE =
        "alluxio.master.embedded.journal.appender.batch.size";
    public static final String MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL =
        "alluxio.master.embedded.journal.heartbeat.interval";
    public static final String MASTER_EMBEDDED_JOURNAL_PORT =
        "alluxio.master.embedded.journal.port";
    public static final String MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL =
        "alluxio.master.embedded.journal.storage.level";
    public static final String MASTER_EMBEDDED_JOURNAL_SHUTDOWN_TIMEOUT =
        "alluxio.master.embedded.journal.shutdown.timeout";
    public static final String MASTER_EMBEDDED_JOURNAL_WRITE_TIMEOUT =
        "alluxio.master.embedded.journal.write.timeout";
    public static final String MASTER_EMBEDDED_JOURNAL_TRIGGERED_SNAPSHOT_WAIT_TIMEOUT =
        "alluxio.master.embedded.journal.triggered.snapshot.wait.timeout";
    public static final String MASTER_EMBEDDED_JOURNAL_TRANSPORT_REQUEST_TIMEOUT_MS =
        "alluxio.master.embedded.journal.transport.request.timeout.ms";
    public static final String MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.master.embedded.journal.transport.max.inbound.message.size";
    public static final String MASTER_KEYTAB_KEY_FILE = "alluxio.master.keytab.file";
    public static final String MASTER_METADATA_SYNC_CONCURRENCY_LEVEL =
        "alluxio.master.metadata.sync.concurrency.level";
    public static final String MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE =
        "alluxio.master.metadata.sync.executor.pool.size";
    public static final String MASTER_METADATA_SYNC_UFS_PREFETCH_POOL_SIZE =
        "alluxio.master.metadata.sync.ufs.prefetch.pool.size";
    public static final String MASTER_METASTORE = "alluxio.master.metastore";
    public static final String MASTER_METASTORE_DIR = "alluxio.master.metastore.dir";
    public static final String MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE =
        "alluxio.master.metastore.inode.cache.evict.batch.size";
    public static final String MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO =
        "alluxio.master.metastore.inode.cache.high.water.mark.ratio";
    public static final String MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO =
        "alluxio.master.metastore.inode.cache.low.water.mark.ratio";
    public static final String MASTER_METASTORE_INODE_CACHE_MAX_SIZE =
        "alluxio.master.metastore.inode.cache.max.size";
    public static final String MASTER_METASTORE_INODE_ITERATION_CRAWLER_COUNT =
        "alluxio.master.metastore.inode.iteration.crawler.count";
    public static final String MASTER_METASTORE_INODE_ENUMERATOR_BUFFER_COUNT =
        "alluxio.master.metastore.inode.enumerator.buffer.count";
    public static final String MASTER_METASTORE_ITERATOR_READAHEAD_SIZE =
        "alluxio.master.metastore.iterator.readahead.size";
    public static final String MASTER_METASTORE_INODE_INHERIT_OWNER_AND_GROUP =
        "alluxio.master.metastore.inode.inherit.owner.and.group";
    public static final String MASTER_PERSISTENCE_CHECKER_INTERVAL_MS =
        "alluxio.master.persistence.checker.interval";
    public static final String MASTER_METRICS_SERVICE_THREADS =
        "alluxio.master.metrics.service.threads";
    public static final String MASTER_METRICS_TIME_SERIES_INTERVAL =
        "alluxio.master.metrics.time.series.interval";
    public static final String MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.master.network.max.inbound.message.size";
    public static final String MASTER_PERSISTENCE_INITIAL_INTERVAL_MS =
        "alluxio.master.persistence.initial.interval";
    public static final String MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS =
        "alluxio.master.persistence.max.total.wait.time";
    public static final String MASTER_PERSISTENCE_MAX_INTERVAL_MS =
        "alluxio.master.persistence.max.interval";
    public static final String MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS =
        "alluxio.master.persistence.scheduler.interval";
    public static final String MASTER_PERSISTENCE_BLACKLIST =
        "alluxio.master.persistence.blacklist";
    public static final String MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL =
        "alluxio.master.log.config.report.heartbeat.interval";
    public static final String MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR =
        "alluxio.master.periodic.block.integrity.check.repair";
    public static final String MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL =
        "alluxio.master.periodic.block.integrity.check.interval";
    public static final String MASTER_PRINCIPAL = "alluxio.master.principal";
    public static final String MASTER_REPLICATION_CHECK_INTERVAL_MS =
        "alluxio.master.replication.check.interval";
    public static final String MASTER_RPC_PORT = "alluxio.master.rpc.port";
    public static final String MASTER_RPC_EXECUTOR_PARALLELISM =
        "alluxio.master.rpc.executor.parallelism";
    public static final String MASTER_RPC_EXECUTOR_MIN_RUNNABLE =
        "alluxio.master.rpc.executor.min.runnable";
    public static final String MASTER_RPC_EXECUTOR_CORE_POOL_SIZE =
        "alluxio.master.rpc.executor.core.pool.size";
    public static final String MASTER_RPC_EXECUTOR_MAX_POOL_SIZE =
        "alluxio.master.rpc.executor.max.pool.size";
    public static final String MASTER_RPC_EXECUTOR_KEEPALIVE =
        "alluxio.master.rpc.executor.keepalive";
    public static final String MASTER_SERVING_THREAD_TIMEOUT =
        "alluxio.master.serving.thread.timeout";
    public static final String MASTER_SKIP_ROOT_ACL_CHECK =
        "alluxio.master.skip.root.acl.check";
    public static final String MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED =
        "alluxio.master.startup.block.integrity.check.enabled";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS =
        "alluxio.master.tieredstore.global.level0.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS =
        "alluxio.master.tieredstore.global.level1.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS =
        "alluxio.master.tieredstore.global.level2.alias";
    public static final String MASTER_TIERED_STORE_GLOBAL_LEVELS =
        "alluxio.master.tieredstore.global.levels";
    public static final String MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE =
        "alluxio.master.tieredstore.global.mediumtype";
    public static final String MASTER_TTL_CHECKER_INTERVAL_MS =
        "alluxio.master.ttl.checker.interval";
    public static final String MASTER_UFS_ACTIVE_SYNC_INTERVAL =
        "alluxio.master.ufs.active.sync.interval";
    public static final String MASTER_UFS_ACTIVE_SYNC_MAX_ACTIVITIES =
        "alluxio.master.ufs.active.sync.max.activities";
    public static final String MASTER_UFS_ACTIVE_SYNC_THREAD_POOL_SIZE =
        "alluxio.master.ufs.active.sync.thread.pool.size";
    public static final String MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT =
        "alluxio.master.ufs.active.sync.poll.timeout";
    public static final String MASTER_UFS_ACTIVE_SYNC_EVENT_RATE_INTERVAL =
        "alluxio.master.ufs.active.sync.event.rate.interval";
    public static final String MASTER_UFS_ACTIVE_SYNC_MAX_AGE =
        "alluxio.master.ufs.active.sync.max.age";
    public static final String MASTER_UFS_ACTIVE_SYNC_INITIAL_SYNC_ENABLED =
        "alluxio.master.ufs.active.sync.initial.sync.enabled";
    public static final String MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT =
        "alluxio.master.ufs.active.sync.retry.timeout";
    public static final String MASTER_UFS_ACTIVE_SYNC_POLL_BATCH_SIZE =
        "alluxio.master.ufs.active.sync.poll.batch.size";
    public static final String MASTER_UFS_BLOCK_LOCATION_CACHE_CAPACITY =
        "alluxio.master.ufs.block.location.cache.capacity";
    public static final String MASTER_UFS_MANAGED_BLOCKING_ENABLED =
        "alluxio.master.ufs.managed.blocking.enabled";
    public static final String MASTER_UFS_PATH_CACHE_CAPACITY =
        "alluxio.master.ufs.path.cache.capacity";
    public static final String MASTER_UFS_PATH_CACHE_THREADS =
        "alluxio.master.ufs.path.cache.threads";
    public static final String MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED =
        "alluxio.master.unsafe.direct.persist.object.enabled";
    public static final String MASTER_UPDATE_CHECK_ENABLED =
        "alluxio.master.update.check.enabled";
    public static final String MASTER_UPDATE_CHECK_INTERVAL =
        "alluxio.master.update.check.interval";
    public static final String MASTER_WEB_BIND_HOST = "alluxio.master.web.bind.host";
    public static final String MASTER_WEB_HOSTNAME = "alluxio.master.web.hostname";
    public static final String MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String MASTER_WHITELIST = "alluxio.master.whitelist";
    public static final String MASTER_WORKER_CONNECT_WAIT_TIME =
        "alluxio.master.worker.connect.wait.time";
    public static final String MASTER_WORKER_INFO_CACHE_REFRESH_TIME
        = "alluxio.master.worker.info.cache.refresh.time";
    public static final String MASTER_WORKER_TIMEOUT_MS = "alluxio.master.worker.timeout";
    public static final String MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES =
        "alluxio.master.journal.checkpoint.period.entries";
    public static final String MASTER_JOURNAL_GC_PERIOD_MS = "alluxio.master.journal.gc.period";
    public static final String MASTER_JOURNAL_GC_THRESHOLD_MS =
        "alluxio.master.journal.gc.threshold";
    public static final String MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS =
        "alluxio.master.journal.temporary.file.gc.threshold";

    //
    // File system master related properties
    //
    public static final String MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE =
        "alluxio.master.filesystem.liststatus.result.message.length";

    //
    // Secondary master related properties
    //
    public static final String SECONDARY_MASTER_METASTORE_DIR =
        "alluxio.secondary.master.metastore.dir";

    //
    // Worker related properties
    //
    public static final String WORKER_ALLOCATOR_CLASS = "alluxio.worker.allocator.class";
    public static final String WORKER_BIND_HOST = "alluxio.worker.bind.host";
    public static final String WORKER_BLOCK_HEARTBEAT_INTERVAL_MS =
        "alluxio.worker.block.heartbeat.interval";
    public static final String WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS =
        "alluxio.worker.block.heartbeat.timeout";
    public static final String WORKER_CONTAINER_HOSTNAME =
        "alluxio.worker.container.hostname";
    public static final String WORKER_DATA_FOLDER = "alluxio.worker.data.folder";
    public static final String WORKER_DATA_FOLDER_PERMISSIONS =
        "alluxio.worker.data.folder.permissions";
    public static final String WORKER_DATA_SERVER_CLASS = "alluxio.worker.data.server.class";
    public static final String WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS =
        "alluxio.worker.data.server.domain.socket.address";
    public static final String WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID =
        "alluxio.worker.data.server.domain.socket.as.uuid";
    public static final String WORKER_DATA_TMP_FOLDER = "alluxio.worker.data.folder.tmp";
    public static final String WORKER_DATA_TMP_SUBDIR_MAX = "alluxio.worker.data.tmp.subdir.max";
    public static final String WORKER_EVICTOR_CLASS = "alluxio.worker.evictor.class";
    public static final String WORKER_BLOCK_ANNOTATOR_CLASS =
        "alluxio.worker.block.annotator.class";
    public static final String WORKER_BLOCK_ANNOTATOR_LRFU_ATTENUATION_FACTOR =
        "alluxio.worker.block.annotator.lrfu.attenuation.factor";
    public static final String WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR =
        "alluxio.worker.block.annotator.lrfu.step.factor";
    public static final String WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES =
        "alluxio.worker.management.tier.align.reserved.bytes";
    public static final String WORKER_MANAGEMENT_BACKOFF_STRATEGY =
        "alluxio.worker.management.backoff.strategy";
    public static final String WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME =
        "alluxio.worker.management.load.detection.cool.down.time";
    public static final String WORKER_MANAGEMENT_TASK_THREAD_COUNT =
        "alluxio.worker.management.task.thread.count";
    public static final String WORKER_MANAGEMENT_BLOCK_TRANSFER_CONCURRENCY_LIMIT =
        "alluxio.worker.management.block.transfer.concurrency.limit";
    public static final String WORKER_MANAGEMENT_TIER_ALIGN_ENABLED =
        "alluxio.worker.management.tier.align.enabled";
    public static final String WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED =
        "alluxio.worker.management.tier.promote.enabled";
    public static final String WORKER_MANAGEMENT_TIER_SWAP_RESTORE_ENABLED =
        "alluxio.worker.management.tier.swap.restore.enabled";
    public static final String WORKER_MANAGEMENT_TIER_ALIGN_RANGE =
        "alluxio.worker.management.tier.align.range";
    public static final String WORKER_MANAGEMENT_TIER_PROMOTE_RANGE =
        "alluxio.worker.management.tier.promote.range";
    public static final String WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT =
        "alluxio.worker.management.tier.promote.quota.percent";
    public static final String WORKER_FILE_BUFFER_SIZE = "alluxio.worker.file.buffer.size";
    public static final String WORKER_FREE_SPACE_TIMEOUT = "alluxio.worker.free.space.timeout";
    public static final String WORKER_HOSTNAME = "alluxio.worker.hostname";
    public static final String WORKER_KEYTAB_FILE = "alluxio.worker.keytab.file";
    public static final String WORKER_MASTER_CONNECT_RETRY_TIMEOUT =
        "alluxio.worker.master.connect.retry.timeout";
    public static final String WORKER_MEMORY_SIZE = "alluxio.worker.memory.size";
    public static final String WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX =
        "alluxio.worker.network.async.cache.manager.threads.max";
    public static final String WORKER_NETWORK_BLOCK_READER_THREADS_MAX =
        "alluxio.worker.network.block.reader.threads.max";
    public static final String WORKER_NETWORK_BLOCK_WRITER_THREADS_MAX =
        "alluxio.worker.network.block.writer.threads.max";
    public static final String WORKER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES =
        "alluxio.worker.network.writer.buffer.size.messages";
    public static final String WORKER_NETWORK_FLOWCONTROL_WINDOW =
        "alluxio.worker.network.flowcontrol.window";
    public static final String WORKER_NETWORK_KEEPALIVE_TIME_MS =
        "alluxio.worker.network.keepalive.time";
    public static final String WORKER_NETWORK_KEEPALIVE_TIMEOUT_MS =
        "alluxio.worker.network.keepalive.timeout";
    public static final String WORKER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.worker.network.max.inbound.message.size";
    public static final String WORKER_NETWORK_NETTY_BOSS_THREADS =
        "alluxio.worker.network.netty.boss.threads";
    public static final String WORKER_NETWORK_NETTY_CHANNEL =
        "alluxio.worker.network.netty.channel";
    public static final String WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD =
        "alluxio.worker.network.netty.shutdown.quiet.period";
    public static final String WORKER_NETWORK_NETTY_WATERMARK_HIGH =
        "alluxio.worker.network.netty.watermark.high";
    public static final String WORKER_NETWORK_NETTY_WATERMARK_LOW =
        "alluxio.worker.network.netty.watermark.low";
    public static final String WORKER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.worker.network.netty.worker.threads";
    public static final String WORKER_NETWORK_READER_BUFFER_SIZE_BYTES =
        "alluxio.worker.network.reader.buffer.size";
    public static final String WORKER_NETWORK_READER_MAX_CHUNK_SIZE_BYTES =
        "alluxio.worker.network.reader.max.chunk.size.bytes";
    public static final String WORKER_NETWORK_SHUTDOWN_TIMEOUT =
        "alluxio.worker.network.shutdown.timeout";
    public static final String WORKER_NETWORK_ZEROCOPY_ENABLED =
        "alluxio.worker.network.zerocopy.enabled";
    public static final String WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE =
        "alluxio.worker.block.master.client.pool.size";
    public static final String WORKER_PRINCIPAL = "alluxio.worker.principal";
    public static final String WORKER_RPC_PORT = "alluxio.worker.rpc.port";
    public static final String WORKER_SESSION_TIMEOUT_MS = "alluxio.worker.session.timeout";
    public static final String WORKER_STORAGE_CHECKER_ENABLED =
        "alluxio.worker.storage.checker.enabled";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCK_READERS =
        "alluxio.worker.tieredstore.block.lock.readers";
    public static final String WORKER_TIERED_STORE_BLOCK_LOCKS =
        "alluxio.worker.tieredstore.block.locks";
    public static final String WORKER_TIERED_STORE_FREE_AHEAD_BYTES =
        "alluxio.worker.tieredstore.free.ahead.bytes";
    public static final String WORKER_TIERED_STORE_LEVELS = "alluxio.worker.tieredstore.levels";
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
    public static final String LOGSERVER_HOSTNAME = "alluxio.logserver.hostname";
    public static final String LOGSERVER_LOGS_DIR = "alluxio.logserver.logs.dir";
    public static final String LOGSERVER_PORT = "alluxio.logserver.port";
    public static final String LOGSERVER_THREADS_MAX = "alluxio.logserver.threads.max";
    public static final String LOGSERVER_THREADS_MIN = "alluxio.logserver.threads.min";

    //
    // User related properties
    //
    public static final String USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES =
        "alluxio.user.block.avoid.eviction.policy.reserved.size.bytes";
    public static final String USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MIN =
        "alluxio.user.block.master.client.pool.size.min";
    public static final String USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX =
        "alluxio.user.block.master.client.pool.size.max";
    public static final String USER_BLOCK_MASTER_CLIENT_POOL_GC_INTERVAL_MS =
        "alluxio.user.block.master.client.pool.gc.interval";
    public static final String USER_BLOCK_MASTER_CLIENT_POOL_GC_THRESHOLD_MS =
        "alluxio.user.block.master.client.pool.gc.threshold";
    public static final String USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.block.remote.read.buffer.size.bytes";
    public static final String USER_BLOCK_SIZE_BYTES_DEFAULT =
        "alluxio.user.block.size.bytes.default";
    public static final String USER_BLOCK_READ_RETRY_SLEEP_MIN =
        "alluxio.user.block.read.retry.sleep.base";
    public static final String USER_BLOCK_READ_RETRY_SLEEP_MAX =
        "alluxio.user.block.read.retry.sleep.max";
    public static final String USER_BLOCK_READ_RETRY_MAX_DURATION =
        "alluxio.user.block.read.retry.max.duration";
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS =
        "alluxio.user.block.worker.client.pool.gc.threshold";
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_MIN =
        "alluxio.user.block.worker.client.pool.min";
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_MAX =
        "alluxio.user.block.worker.client.pool.max";
    public static final String USER_BLOCK_WORKER_CLIENT_POOL_SIZE =
        "alluxio.user.block.worker.client.pool.size";
    public static final String USER_BLOCK_WORKER_CLIENT_READ_RETRY =
        "alluxio.user.block.worker.client.read.retry";
    public static final String USER_BLOCK_WRITE_LOCATION_POLICY =
        "alluxio.user.block.write.location.policy.class";
    public static final String USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED =
        "alluxio.user.client.cache.async.write.enabled";
    public static final String USER_CLIENT_CACHE_ASYNC_WRITE_THREADS =
        "alluxio.user.client.cache.async.write.threads";
    public static final String USER_CLIENT_CACHE_ENABLED =
        "alluxio.user.client.cache.enabled";
    public static final String USER_CLIENT_CACHE_EVICTOR_CLASS =
        "alluxio.user.client.cache.evictor.class";
    public static final String USER_CLIENT_CACHE_EVICTOR_LFU_LOGBASE =
        "alluxio.user.client.cache.evictor.lfu.logbase";
    public static final String USER_CLIENT_CACHE_DIR =
        "alluxio.user.client.cache.dir";
    public static final String USER_CLIENT_CACHE_PAGE_SIZE =
        "alluxio.user.client.cache.page.size";
    public static final String USER_CLIENT_CACHE_LOCAL_STORE_FILE_BUCKETS =
        "alluxio.user.client.cache.local.store.file.buckets";
    public static final String USER_CLIENT_CACHE_SIZE =
        "alluxio.user.client.cache.size";
    public static final String USER_CLIENT_CACHE_STORE_TYPE =
        "alluxio.user.client.cache.store.type";
    public static final String USER_CONF_CLUSTER_DEFAULT_ENABLED =
        "alluxio.user.conf.cluster.default.enabled";
    public static final String USER_CONF_SYNC_INTERVAL = "alluxio.user.conf.sync.interval";
    public static final String USER_DATE_FORMAT_PATTERN = "alluxio.user.date.format.pattern";
    public static final String USER_DIRECT_MEMORY_IO_ENABLED =
        "alluxio.user.direct.memory.io.enabled";
    public static final String USER_FILE_BUFFER_BYTES = "alluxio.user.file.buffer.bytes";
    public static final String USER_FILE_RESERVED_BYTES = "alluxio.user.file.reserved.bytes";
    public static final String USER_FILE_COPYFROMLOCAL_BLOCK_LOCATION_POLICY =
        "alluxio.user.file.copyfromlocal.block.location.policy.class";
    public static final String USER_FILE_DELETE_UNCHECKED =
        "alluxio.user.file.delete.unchecked";
    public static final String USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN =
        "alluxio.user.file.master.client.pool.size.min";
    public static final String USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX =
        "alluxio.user.file.master.client.pool.size.max";
    public static final String USER_FILE_MASTER_CLIENT_POOL_GC_INTERVAL_MS =
        "alluxio.user.file.master.client.pool.gc.interval";
    public static final String USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS =
        "alluxio.user.file.master.client.pool.gc.threshold";
    public static final String USER_FILE_METADATA_LOAD_TYPE =
        "alluxio.user.file.metadata.load.type";
    public static final String USER_FILE_METADATA_SYNC_INTERVAL =
        "alluxio.user.file.metadata.sync.interval";
    public static final String USER_FILE_PASSIVE_CACHE_ENABLED =
        "alluxio.user.file.passive.cache.enabled";
    public static final String USER_FILE_READ_TYPE_DEFAULT = "alluxio.user.file.readtype.default";
    public static final String USER_FILE_PERSIST_ON_RENAME = "alluxio.user.file.persist.on.rename";
    public static final String USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME =
        "alluxio.user.file.persistence.initial.wait.time";
    public static final String USER_FILE_REPLICATION_MAX = "alluxio.user.file.replication.max";
    public static final String USER_FILE_REPLICATION_MIN = "alluxio.user.file.replication.min";
    public static final String USER_FILE_TARGET_MEDIA = "alluxio.user.file.target.media";
    public static final String USER_FILE_REPLICATION_DURABLE =
        "alluxio.user.file.replication.durable";
    public static final String USER_FILE_SEQUENTIAL_PREAD_THRESHOLD =
        "alluxio.user.file.sequential.pread.threshold";
    public static final String USER_FILE_UFS_TIER_ENABLED = "alluxio.user.file.ufs.tier.enabled";
    public static final String USER_FILE_WAITCOMPLETED_POLL_MS =
        "alluxio.user.file.waitcompleted.poll";
    public static final String USER_FILE_CREATE_TTL =
        "alluxio.user.file.create.ttl";
    public static final String USER_FILE_CREATE_TTL_ACTION =
        "alluxio.user.file.create.ttl.action";
    public static final String USER_FILE_WRITE_TYPE_DEFAULT = "alluxio.user.file.writetype.default";
    public static final String USER_FILE_WRITE_TIER_DEFAULT =
        "alluxio.user.file.write.tier.default";
    public static final String USER_HOSTNAME = "alluxio.user.hostname";
    public static final String USER_LOCAL_READER_CHUNK_SIZE_BYTES =
        "alluxio.user.local.reader.chunk.size.bytes";
    public static final String USER_LOCAL_WRITER_CHUNK_SIZE_BYTES =
        "alluxio.user.local.writer.chunk.size.bytes";
    public static final String USER_LOGGING_THRESHOLD = "alluxio.user.logging.threshold";
    public static final String USER_METADATA_CACHE_ENABLED =
        "alluxio.user.metadata.cache.enabled";
    public static final String USER_METADATA_CACHE_MAX_SIZE =
        "alluxio.user.metadata.cache.max.size";
    public static final String USER_METADATA_CACHE_EXPIRATION_TIME =
        "alluxio.user.metadata.cache.expiration.time";
    public static final String USER_METRICS_COLLECTION_ENABLED =
        "alluxio.user.metrics.collection.enabled";
    public static final String USER_METRICS_HEARTBEAT_INTERVAL_MS =
        "alluxio.user.metrics.heartbeat.interval";
    public static final String USER_APP_ID = "alluxio.user.app.id";
    public static final String USER_NETWORK_DATA_TIMEOUT =
        "alluxio.user.network.data.timeout";
    public static final String USER_NETWORK_READER_BUFFER_SIZE_MESSAGES =
        "alluxio.user.network.reader.buffer.size.messages";
    public static final String USER_NETWORK_READER_CHUNK_SIZE_BYTES =
        "alluxio.user.network.reader.chunk.size.bytes";
    public static final String USER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES =
        "alluxio.user.network.writer.buffer.size.messages";
    public static final String USER_NETWORK_WRITER_CHUNK_SIZE_BYTES =
        "alluxio.user.network.writer.chunk.size.bytes";
    public static final String USER_NETWORK_WRITER_CLOSE_TIMEOUT =
        "alluxio.user.network.writer.close.timeout";
    public static final String USER_NETWORK_WRITER_FLUSH_TIMEOUT =
        "alluxio.user.network.writer.flush.timeout";
    public static final String USER_NETWORK_ZEROCOPY_ENABLED =
        "alluxio.user.network.zerocopy.enabled";
    public static final String USER_STREAMING_DATA_TIMEOUT =
        "alluxio.user.streaming.data.timeout";
    public static final String USER_STREAMING_READER_BUFFER_SIZE_MESSAGES =
        "alluxio.user.streaming.reader.buffer.size.messages";
    public static final String USER_STREAMING_READER_CHUNK_SIZE_BYTES =
        "alluxio.user.streaming.reader.chunk.size.bytes";
    public static final String USER_STREAMING_WRITER_BUFFER_SIZE_MESSAGES =
        "alluxio.user.streaming.writer.buffer.size.messages";
    public static final String USER_STREAMING_WRITER_CHUNK_SIZE_BYTES =
        "alluxio.user.streaming.writer.chunk.size.bytes";
    public static final String USER_STREAMING_WRITER_CLOSE_TIMEOUT =
        "alluxio.user.streaming.writer.close.timeout";
    public static final String USER_STREAMING_WRITER_FLUSH_TIMEOUT =
        "alluxio.user.streaming.writer.flush.timeout";
    public static final String USER_STREAMING_ZEROCOPY_ENABLED =
        "alluxio.user.streaming.zerocopy.enabled";
    public static final String USER_NETWORK_FLOWCONTROL_WINDOW =
        "alluxio.user.network.flowcontrol.window";
    public static final String USER_NETWORK_KEEPALIVE_TIME =
        "alluxio.user.network.keepalive.time";
    public static final String USER_NETWORK_KEEPALIVE_TIMEOUT =
        "alluxio.user.network.keepalive.timeout";
    public static final String USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.user.network.max.inbound.message.size";
    public static final String USER_NETWORK_NETTY_CHANNEL =
        "alluxio.user.network.netty.channel";
    public static final String USER_NETWORK_NETTY_WORKER_THREADS =
        "alluxio.user.network.netty.worker.threads";
    public static final String USER_NETWORK_RPC_FLOWCONTROL_WINDOW =
        "alluxio.user.network.rpc.flowcontrol.window";
    public static final String USER_NETWORK_RPC_KEEPALIVE_TIME =
        "alluxio.user.network.rpc.keepalive.time";
    public static final String USER_NETWORK_RPC_KEEPALIVE_TIMEOUT =
        "alluxio.user.network.rpc.keepalive.timeout";
    public static final String USER_NETWORK_RPC_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.user.network.rpc.max.inbound.message.size";
    public static final String USER_NETWORK_RPC_NETTY_CHANNEL =
        "alluxio.user.network.rpc.netty.channel";
    public static final String USER_NETWORK_RPC_NETTY_WORKER_THREADS =
        "alluxio.user.network.rpc.netty.worker.threads";
    public static final String USER_NETWORK_RPC_MAX_CONNECTIONS =
        "alluxio.user.network.rpc.max.connections";
    public static final String USER_NETWORK_STREAMING_FLOWCONTROL_WINDOW =
        "alluxio.user.network.streaming.flowcontrol.window";
    public static final String USER_NETWORK_STREAMING_KEEPALIVE_TIME =
        "alluxio.user.network.streaming.keepalive.time";
    public static final String USER_NETWORK_STREAMING_KEEPALIVE_TIMEOUT =
        "alluxio.user.network.streaming.keepalive.timeout";
    public static final String USER_NETWORK_STREAMING_MAX_INBOUND_MESSAGE_SIZE =
        "alluxio.user.network.streaming.max.inbound.message.size";
    public static final String USER_NETWORK_STREAMING_NETTY_CHANNEL =
        "alluxio.user.network.streaming.netty.channel";
    public static final String USER_NETWORK_STREAMING_NETTY_WORKER_THREADS =
        "alluxio.user.network.streaming.netty.worker.threads";
    public static final String USER_NETWORK_STREAMING_MAX_CONNECTIONS =
        "alluxio.user.network.streaming.max.connections";
    public static final String USER_RPC_RETRY_BASE_SLEEP_MS =
        "alluxio.user.rpc.retry.base.sleep";
    public static final String USER_RPC_RETRY_MAX_DURATION =
        "alluxio.user.rpc.retry.max.duration";
    public static final String USER_RPC_RETRY_MAX_SLEEP_MS = "alluxio.user.rpc.retry.max.sleep";
    public static final String USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED =
        "alluxio.user.ufs.block.location.all.fallback.enabled";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY =
        "alluxio.user.ufs.block.read.location.policy";
    public static final String USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS =
        "alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards";
    public static final String USER_UFS_BLOCK_READ_CONCURRENCY_MAX =
        "alluxio.user.ufs.block.read.concurrency.max";
    public static final String USER_UPDATE_FILE_ACCESSTIME_DISABLED =
        "alluxio.user.update.file.accesstime.disabled";
    public static final String USER_SHORT_CIRCUIT_ENABLED = "alluxio.user.short.circuit.enabled";
    public static final String USER_SHORT_CIRCUIT_PREFERRED =
        "alluxio.user.short.circuit.preferred";
    public static final String USER_WORKER_LIST_REFRESH_INTERVAL =
        "alluxio.user.worker.list.refresh.interval";

    //
    // FUSE integration related properties
    //
    public static final String FUSE_CACHED_PATHS_MAX = "alluxio.fuse.cached.paths.max";
    public static final String FUSE_DEBUG_ENABLED = "alluxio.fuse.debug.enabled";
    public static final String FUSE_FS_NAME = "alluxio.fuse.fs.name";
    public static final String FUSE_JNIFUSE_ENABLED = "alluxio.fuse.jnifuse.enabled";
    public static final String FUSE_LOGGING_THRESHOLD = "alluxio.fuse.logging.threshold";
    public static final String FUSE_MAXWRITE_BYTES = "alluxio.fuse.maxwrite.bytes";
    public static final String FUSE_MAXCACHE_BYTES = "alluxio.fuse.maxcache.bytes";
    public static final String FUSE_USER_GROUP_TRANSLATION_ENABLED =
        "alluxio.fuse.user.group.translation.enabled";

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
    public static final String AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD =
        "alluxio.security.stale.channel.purge.interval";

    //
    // Job service
    //
    public static final String JOB_MASTER_CLIENT_THREADS =
        "alluxio.job.master.client.threads";
    public static final String JOB_MASTER_FINISHED_JOB_PURGE_COUNT =
        "alluxio.job.master.finished.job.purge.count";
    public static final String JOB_MASTER_FINISHED_JOB_RETENTION_TIME =
        "alluxio.job.master.finished.job.retention.time";
    public static final String JOB_MASTER_JOB_CAPACITY = "alluxio.job.master.job.capacity";
    public static final String JOB_MASTER_WORKER_HEARTBEAT_INTERVAL =
        "alluxio.job.master.worker.heartbeat.interval";
    public static final String JOB_MASTER_WORKER_TIMEOUT =
        "alluxio.job.master.worker.timeout";

    public static final String JOB_MASTER_BIND_HOST = "alluxio.job.master.bind.host";
    public static final String JOB_MASTER_HOSTNAME = "alluxio.job.master.hostname";
    public static final String JOB_MASTER_LOST_WORKER_INTERVAL =
        "alluxio.job.master.lost.worker.interval";
    public static final String JOB_MASTER_RPC_PORT = "alluxio.job.master.rpc.port";
    public static final String JOB_MASTER_WEB_BIND_HOST = "alluxio.job.master.web.bind.host";
    public static final String JOB_MASTER_WEB_HOSTNAME = "alluxio.job.master.web.hostname";
    public static final String JOB_MASTER_WEB_PORT = "alluxio.job.master.web.port";

    public static final String JOB_MASTER_RPC_ADDRESSES = "alluxio.job.master.rpc.addresses";
    public static final String JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES =
        "alluxio.job.master.embedded.journal.addresses";
    public static final String JOB_MASTER_EMBEDDED_JOURNAL_PORT =
        "alluxio.job.master.embedded.journal.port";

    public static final String JOB_WORKER_BIND_HOST = "alluxio.job.worker.bind.host";
    public static final String JOB_WORKER_DATA_PORT = "alluxio.job.worker.data.port";
    public static final String JOB_WORKER_HOSTNAME = "alluxio.job.worker.hostname";
    public static final String JOB_WORKER_RPC_PORT = "alluxio.job.worker.rpc.port";
    public static final String JOB_WORKER_THREADPOOL_SIZE = "alluxio.job.worker.threadpool.size";
    public static final String JOB_WORKER_THROTTLING = "alluxio.job.worker.throttling";
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

    //
    // Table service properties
    //
    public static final String TABLE_ENABLED = "alluxio.table.enabled";
    public static final String TABLE_CATALOG_PATH = "alluxio.table.catalog.path";
    public static final String TABLE_CATALOG_UDB_SYNC_TIMEOUT =
        "alluxio.table.catalog.udb.sync.timeout";
    public static final String TABLE_TRANSFORM_MANAGER_JOB_MONITOR_INTERVAL =
        "alluxio.table.transform.manager.job.monitor.interval";
    public static final String TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME =
        "alluxio.table.transform.manager.job.history.retention.time";

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
    // TODO(binfan): use alluxio.worker.tieredstore.levelX.mediatype instead
    WORKER_TIERED_STORE_LEVEL_ALIAS("alluxio.worker.tieredstore.level%d.alias",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.alias"),
    WORKER_TIERED_STORE_LEVEL_DIRS_PATH("alluxio.worker.tieredstore.level%d.dirs.path",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.path"),
    WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE("alluxio.worker.tieredstore.level%d.dirs.mediumtype",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.mediumtype"),
    WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA("alluxio.worker.tieredstore.level%d.dirs.quota",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.quota"),
    WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO(
        "alluxio.worker.tieredstore.level%d.watermark.high.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.watermark\\.high\\.ratio"),
    WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO(
        "alluxio.worker.tieredstore.level%d.watermark.low.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.watermark\\.low\\.ratio"),
    USER_NETWORK_KEEPALIVE_TIME_MS("alluxio.user.network.%s.keepalive.time",
        "alluxio\\.user\\.network\\.(\\w+)\\.keepalive\\.time"),
    USER_NETWORK_KEEPALIVE_TIMEOUT_MS ("alluxio.user.network.%s.keepalive.timeout",
        "alluxio\\.user\\.network\\.(\\w+)\\.keepalive\\.timeout"),
    USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE("alluxio.user.network.%s.max.inbound.message.size",
        "alluxio\\.user\\.network\\.(\\w+)\\.max\\.inbound\\.message\\.size"),
    USER_NETWORK_FLOWCONTROL_WINDOW("alluxio.user.network.%s.flowcontrol.window",
        "alluxio\\.user\\.network\\.(\\w+)\\.flowcontrol\\.window"),
    USER_NETWORK_NETTY_CHANNEL("alluxio.user.network.%s.netty.channel",
        "alluxio\\.user\\.network\\.(\\w+)\\.netty\\.channel"),
    USER_NETWORK_NETTY_WORKER_THREADS("alluxio.user.network.%s.netty.worker.threads",
        "alluxio\\.user\\.network\\.(\\w+)\\.netty\\.worker\\.threads"),
    USER_NETWORK_MAX_CONNECTIONS("alluxio.user.network.%s.max.connections",
        "alluxio\\.user\\.network\\.(\\w+)\\.max\\.connections"),

    /**
     * @deprecated This template is always deprecated. It is used only for testing.
     */
    @Deprecated(message = "testDeprecatedMsg")
    TEST_DEPRECATED_TEMPLATE(
        "alluxio.test.%s.format.deprecated.template",
        "alluxio\\.test\\.(\\w+)\\.format\\.deprecated\\.template"),
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
      return MoreObjects.toStringHelper(this).add("format", mFormat).add("pattern", mPattern)
          .toString();
    }

    /**
     * Converts a property key template (e.g.,
     * {@link #WORKER_TIERED_STORE_LEVEL_ALIAS}) to a {@link PropertyKey} instance.
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
    // Regex matching for templates can be expensive when checking properties frequently.
    // Use a cache to store regexp matching results to reduce CPU overhead.
    Boolean result = REGEXP_CACHE.getIfPresent(input);
    if (result != null) {
      return result;
    }
    // Check if input matches any parameterized keys
    result = false;
    for (Template template : Template.values()) {
      if (template.matches(input)) {
        result = true;
        break;
      }
    }
    REGEXP_CACHE.put(input, result);
    return result;
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

    if (isRemoved(input)) {
      String errorMsg = String.format("%s is no longer a valid property. %s", input,
          PropertyKey.getRemovalMessage(input));
      LOG.error(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    } else {
      throw new IllegalArgumentException(
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(input));
    }
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

  /**
   * @param name name of the property
   * @return the registered property key if found, or else create a new one and return
   */
  public static PropertyKey getOrBuildCustom(String name) {
    return DEFAULT_KEYS_MAP.computeIfAbsent(name,
        (key) -> new Builder(key).setIsBuiltIn(false).buildUnregistered());
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

  private static final DeprecatedKeyChecker DEPRECATED_CHECKER = new DeprecatedKeyChecker();

  /**
   * Returns whether or not the given property key is marked as deprecated.
   *
   * It first checks if the specific key is deprecated, otherwise it will fall back to checking
   * if the key's name matches any of the PropertyKey templates. If no keys or templates match, it
   * will return false. This will only return true when the key is marked with a {@link Deprecated}
   * annotation.
   *
   * @param key the property key to check
   * @return if this property key is deprecated
   * @see Deprecated
   * @see #getDeprecationMessage(PropertyKey)
   */
  public static boolean isDeprecated(PropertyKey key) {
    return DEPRECATED_CHECKER.hasAnnotation(key);
  }

  /**
   * @param name the property key to check
   * @return if this property key is deprecated
   */
  public static boolean isDeprecated(String name) {
    return isDeprecated(PropertyKey.fromString(name));
  }

  /**
   * Returns whether or not a property key has been removed from use.
   *
   * If a PropertyKey or {@link Template} is deemed as "Removed" it will exist within
   * {@link RemovedKey}. This method can be used to detect if a key being utilized has been removed.
   *
   * @param key the property key to check
   * @return true this property key is removed, false otherwise
   * @see RemovedKey
   * @see #isDeprecated(alluxio.conf.PropertyKey)
   * @see Deprecated
   */
  public static boolean isRemoved(String key) {
    return RemovedKey.isRemoved(key);
  }

  /**
   * @param key the property key to get the deprecation message from
   * @return the message, or empty string is the property key isn't deprecated
   */
  public static String getDeprecationMessage(PropertyKey key) {
    if (isDeprecated(key)) {
      Deprecated annotation = DEPRECATED_CHECKER.getAnnotation(key);
      if (annotation != null) {
        return annotation.message();
      }
    }
    return "";
  }

  /**
   * @param key the property key to get the removal message from
   * @return the message, or empty string is the property key isn't removed
   */
  public static String getRemovalMessage(String key) {
    String msg = RemovedKey.getMessage(key);
    return msg == null ? "" : msg;
  }
}
