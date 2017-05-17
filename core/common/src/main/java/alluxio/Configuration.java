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
import alluxio.exception.PreconditionMessage;
import alluxio.network.ChannelType;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.OSUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.management.OperatingSystemMXBean;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * All the runtime configuration properties of Alluxio. This class works like a dictionary and
 * serves each Alluxio configuration property as a key-value pair.
 *
 * <p>
 * Alluxio configuration properties are loaded into this class in the following order with
 * decreasing priority:
 * <ol>
 * <li>Java system properties;</li>
 * <li>Environment variables via {@code alluxio-env.sh} or from OS settings;</li>
 * <li>Site specific properties via {@code alluxio-site.properties} file;</li>
 * <li>Default properties via {@code alluxio-default.properties} file.</li>
 * </ol>
 *
 * <p>
 * The default properties are defined in a property file {@code alluxio-default.properties}
 * distributed with Alluxio jar. Alluxio users can override values of these default properties by
 * creating {@code alluxio-site.properties} and putting it under java {@code CLASSPATH} when running
 * Alluxio (e.g., ${ALLUXIO_HOME}/conf/)
 */
@NotThreadSafe
public final class Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

  /** Regex string to find "${key}" for variable substitution. */
  private static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  /** Regex to find ${key} for variable substitution. */
  private static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);
  /** Map of properties. */
  private static final ConcurrentHashMapV8<String, String> PROPERTIES = new ConcurrentHashMapV8<>();

  /** File to set customized properties for Alluxio server (both master and worker) and client. */
  public static final String SITE_PROPERTIES = "alluxio-site.properties";

  static {
    init();
  }

  /**
   * Initializes the default {@link Configuration}.
   *
   * The order of preference is (1) system properties, (2) properties in the specified file, (3)
   * default property values.
   */
  static void init() {
    // Load default
    Properties defaultProps = createDefaultProps();

    // Load system properties
    Properties systemProps = new Properties();
    systemProps.putAll(System.getProperties());

    // Now lets combine, order matters here
    PROPERTIES.clear();
    merge(defaultProps);
    merge(systemProps);

    // Load site specific properties file if not in test mode. Note that we decide whether in test
    // mode by default properties and system properties (via getBoolean). If it is not in test mode
    // the PROPERTIES will be updated again.
    if (!getBoolean(PropertyKey.TEST_MODE)) {
      String confPaths = get(PropertyKey.SITE_CONF_DIR);
      String[] confPathList = confPaths.split(",");
      Properties siteProps = ConfigurationUtils.searchPropertiesFile(SITE_PROPERTIES, confPathList);
      // Update site properties and system properties in order
      if (siteProps != null) {
        merge(siteProps);
        merge(systemProps);
      }
    }

    // TODO(andrew): get rid of the MASTER_ADDRESS property key
    if (containsKey(PropertyKey.MASTER_HOSTNAME)) {
      String masterHostname = get(PropertyKey.MASTER_HOSTNAME);
      String masterPort = get(PropertyKey.MASTER_RPC_PORT);
      boolean useZk = Boolean.parseBoolean(get(PropertyKey.ZOOKEEPER_ENABLED));
      String masterAddress =
          (useZk ? Constants.HEADER_FT : Constants.HEADER) + masterHostname + ":" + masterPort;
      set(PropertyKey.MASTER_ADDRESS, masterAddress);
    }

    validate();
  }

  /**
   * @return default properties
   */
  private static Properties createDefaultProps() {
    Properties defaultProps = new Properties();
    // Load compile-time default
    for (PropertyKey key : PropertyKey.defaultKeys()) {
      String value = key.getDefaultValue();
      if (value != null) {
        defaultProps.setProperty(key.toString(), value);
      }
    }

    // Load run-time default
    defaultProps.setProperty(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL.toString(),
        String.valueOf(ChannelType.defaultType()));
    defaultProps.setProperty(PropertyKey.USER_NETWORK_NETTY_CHANNEL.toString(),
        String.valueOf(ChannelType.defaultType()));
    // Set ramdisk volume according to OS type
    if (OSUtils.isLinux()) {
      defaultProps
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH.toString(), "/mnt/ramdisk");
    } else if (OSUtils.isMacOS()) {
      defaultProps.setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH.toString(),
          "/Volumes/ramdisk");
    }
    // Set a reasonable default size for worker memory
    try {
      OperatingSystemMXBean operatingSystemMXBean =
          (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      long memSize = operatingSystemMXBean.getTotalPhysicalMemorySize();
      defaultProps
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE.toString(), String.valueOf(memSize * 2 / 3));
    } catch (Exception e) {
      // The package com.sun.management may not be available on every platform.
      // fallback to the compile-time default value
    }
    return defaultProps;
  }

  /**
   * Merges the current configuration properties with alternate properties. A property from the new
   * configuration wins if it also appears in the current configuration.
   *
   * @param properties The source {@link Properties} to be merged
   */
  public static void merge(Map<?, ?> properties) {
    if (properties != null) {
      // merge the system properties
      for (Map.Entry<?, ?> entry : properties.entrySet()) {
        String key = entry.getKey().toString();
        String value = entry.getValue().toString();
        if (PropertyKey.isValid(key)) {
          PROPERTIES.put(key, value);
        }
      }
    }
    checkUserFileBufferBytes();
  }

  // Public accessor methods

  // TODO(binfan): this method should be hidden and only used during initialization and tests.

  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   */
  public static void set(PropertyKey key, Object value) {
    Preconditions.checkArgument(key != null && value != null,
        String.format("the key value pair (%s, %s) cannot have null", key, value));
    PROPERTIES.put(key.toString(), value.toString());
    checkUserFileBufferBytes();
  }

  /**
   * Unsets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to unset
   */
  public static void unset(PropertyKey key) {
    Preconditions.checkNotNull(key, "key");
    PROPERTIES.remove(key.toString());
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  public static String get(PropertyKey key) {
    String rawValue = PROPERTIES.get(key.toString());
    if (rawValue == null) {
      // if key is not found among the default properties
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }
    return lookup(rawValue);
  }

  /**
   * Checks if the {@link Properties} contains the given key.
   *
   * @param key the key to check
   * @return true if the key is in the {@link Properties}, false otherwise
   */
  public static boolean containsKey(PropertyKey key) {
    return PROPERTIES.containsKey(key.toString());
  }

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  public static int getInt(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Integer.parseInt(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_INTEGER.getMessage(key));
    }
  }

  /**
   * Gets the long representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code long}
   */
  public static long getLong(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Long.parseLong(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_LONG.getMessage(key));
    }
  }

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  public static double getDouble(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Double.parseDouble(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_DOUBLE.getMessage(key));
    }
  }

  /**
   * Gets the float representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code float}
   */
  public static float getFloat(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Float.parseFloat(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_FLOAT.getMessage(key));
    }
  }

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  public static boolean getBoolean(PropertyKey key) {
    String rawValue = get(key);

    if (rawValue.equalsIgnoreCase("true")) {
      return true;
    } else if (rawValue.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_BOOLEAN.getMessage(key));
    }
  }

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @param delimiter the delimiter to split the values
   * @return the list of values for the given key
   */
  public static List<String> getList(PropertyKey key, String delimiter) {
    Preconditions.checkArgument(delimiter != null,
        "Illegal separator for Alluxio properties as list");
    String rawValue = get(key);

    return Lists.newLinkedList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
        .split(rawValue));
  }

  /**
   * Gets the value for the given key as an enum value.
   *
   * @param key the key to get the value for
   * @param enumType the type of the enum
   * @param <T> the type of the enum
   * @return the value for the given key as an enum value
   */
  public static <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    String rawValue = get(key);

    return Enum.valueOf(enumType, rawValue);
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  public static long getBytes(PropertyKey key) {
    String rawValue = get(key);

    try {
      return FormatUtils.parseSpaceSize(rawValue);
    } catch (Exception ex) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_BYTES.getMessage(key));
    }
  }

  /**
   * Gets the time of key in millisecond unit.
   *
   * @param key the key to get the value for
   * @return the time of key in millisecond unit
   */
  public static long getMs(PropertyKey key) {
    String rawValue = get(key);
    try {
      return FormatUtils.parseTimeSize(rawValue);
    } catch (Exception e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_MS.getMessage(key));
    }
  }

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  public static <T> Class<T> getClass(PropertyKey key) {
    String rawValue = get(key);

    try {
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(rawValue);
      return clazz;
    } catch (Exception e) {
      LOG.error("requested class could not be loaded: {}", rawValue, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a set of properties that share a given common prefix key as a map. E.g., if A.B=V1 and
   * A.C=V2, calling this method with prefixKey=A returns a map of {B=V1, C=V2}, where B and C are
   * also valid properties. If no property shares the prefix, an empty map is returned.
   *
   * @param prefixKey the prefix key
   * @return a map from nested properties aggregated by the prefix
   */
  public static Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    Map<String, String> ret = Maps.newHashMap();
    for (Map.Entry<String, String> entry: PROPERTIES.entrySet()) {
      String key = entry.getKey();
      if (prefixKey.isNested(key)) {
        String suffixKey = key.substring(prefixKey.length() + 1);
        ret.put(suffixKey, entry.getValue());
      }
    }
    return ret;
  }

  /**
   * @return a view of the internal {@link Properties} of as an immutable map
   */
  public static Map<String, String> toMap() {
    return Collections.unmodifiableMap(PROPERTIES);
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base string to look for
   * @return the key name with the ${key} substituted
   */
  private static String lookup(String base) {
    return lookupRecursively(base, new HashMap<String, String>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the String to look for
   * @param found {@link Map} of String that already seen in this path
   * @return resolved String value
   */
  private static String lookupRecursively(final String base, Map<String, String> found) {
    // check argument
    if (base == null) {
      return null;
    }

    String resolved = base;
    // Lets find pattern match to ${key}.
    // TODO(hsaputra): Consider using Apache Commons StrSubstitutor.
    Matcher matcher = CONF_REGEX.matcher(base);
    while (matcher.find()) {
      String match = matcher.group(2).trim();
      String value;
      if (!found.containsKey(match)) {
        value = lookupRecursively(PROPERTIES.get(match), found);
        found.put(match, value);
      } else {
        value = found.get(match);
      }
      if (value != null) {
        LOG.debug("Replacing {} with {}", matcher.group(1), value);
        resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(value));
      }
    }
    return resolved;
  }

  /**
   * Validates worker port configuration.
   *
   * @throws IllegalStateException if invalid worker port configuration is encountered
   */
  private static void checkWorkerPorts() {
    int maxWorkersPerHost = getInt(PropertyKey.INTEGRATION_YARN_WORKERS_PER_HOST_MAX);
    if (maxWorkersPerHost > 1) {
      String message = "%s cannot be specified when allowing multiple workers per host with "
          + PropertyKey.Name.INTEGRATION_YARN_WORKERS_PER_HOST_MAX + "=" + maxWorkersPerHost;
      Preconditions.checkState(System.getProperty(PropertyKey.Name.WORKER_DATA_PORT) == null,
          String.format(message, PropertyKey.WORKER_DATA_PORT));
      Preconditions.checkState(System.getProperty(PropertyKey.Name.WORKER_RPC_PORT) == null,
          String.format(message, PropertyKey.WORKER_RPC_PORT));
      Preconditions.checkState(System.getProperty(PropertyKey.Name.WORKER_WEB_PORT) == null,
          String.format(message, PropertyKey.WORKER_WEB_PORT));
      set(PropertyKey.WORKER_DATA_PORT, "0");
      set(PropertyKey.WORKER_RPC_PORT, "0");
      set(PropertyKey.WORKER_WEB_PORT, "0");
    }
  }

  /**
   * Validates the user file buffer size is a non-negative number.
   *
   * @throws IllegalStateException if invalid user file buffer size configuration is encountered
   */
  private static void checkUserFileBufferBytes() {
    if (!containsKey(PropertyKey.USER_FILE_BUFFER_BYTES)) { // load from hadoop conf
      return;
    }
    long usrFileBufferBytes = getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
    Preconditions.checkState((usrFileBufferBytes & Integer.MAX_VALUE) == usrFileBufferBytes,
        PreconditionMessage.INVALID_USER_FILE_BUFFER_BYTES.toString(),
        PropertyKey.Name.USER_FILE_BUFFER_BYTES, usrFileBufferBytes);
  }

  /**
   * Validates Zookeeper-related configuration and prints warnings for possible sources of error.
   *
   * @throws IllegalStateException if invalid Zookeeper configuration is encountered
   */
  private static void checkZkConfiguration() {
    Preconditions.checkState(
        containsKey(PropertyKey.ZOOKEEPER_ADDRESS) == getBoolean(PropertyKey.ZOOKEEPER_ENABLED),
        PreconditionMessage.INCONSISTENT_ZK_CONFIGURATION.toString(),
        PropertyKey.Name.ZOOKEEPER_ADDRESS, PropertyKey.Name.ZOOKEEPER_ENABLED);
  }

  /**
   * Validates the configuration.
   *
   * @throws IllegalStateException if invalid configuration is encountered
   */
  public static void validate() {
    for (Map.Entry<String, String> entry : toMap().entrySet()) {
      String propertyName = entry.getKey();
      Preconditions.checkState(PropertyKey.isValid(propertyName), propertyName);
    }
    checkWorkerPorts();
    checkUserFileBufferBytes();
    checkZkConfiguration();
  }

  private Configuration() {} // prevent instantiation
}
