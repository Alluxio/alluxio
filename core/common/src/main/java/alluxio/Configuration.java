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

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * Global configuration properties of Alluxio. This class works like a dictionary and serves each
 * Alluxio configuration property as a key-value pair.
 *
 * <p>
 * Alluxio configuration properties are loaded into this class in the following order with
 * decreasing priority:
 * <ol>
 * <li>Java system properties;</li>
 * <li>Environment variables via {@code alluxio-env.sh} or from OS settings;</li>
 * <li>Site specific properties via {@code alluxio-site.properties} file;</li>
 * </ol>
 *
 * <p>
 * The default properties are defined in the {@link PropertyKey} class in the codebase. Alluxio
 * users can override values of these default properties by creating {@code alluxio-site.properties}
 * and putting it under java {@code CLASSPATH} when running Alluxio (e.g., ${ALLUXIO_HOME}/conf/)
 *
 * <p>
 * This class defines many convenient static methods which delegate to an internal
 * {@link InstancedConfiguration}. To use this global configuration in a method that takes
 * {@link AlluxioConfiguration} as an argument, pass {@link Configuration#global()}.
 */
@NotThreadSafe
public final class Configuration {
  private static final AlluxioProperties PROPERTIES = new AlluxioProperties();
  private static final InstancedConfiguration CONF = new InstancedConfiguration(PROPERTIES);

  static {
    reset();
  }

  /**
   * Resets {@link Configuration} back to the default one.
   */
  public static void reset() {
    // Step1: bootstrap the configuration. This is necessary because we need to resolve alluxio.home
    // (likely to be in system properties) to locate the conf dir to search for the site property
    // file.
    PROPERTIES.clear();
    PROPERTIES.merge(System.getProperties(), Source.SYSTEM_PROPERTY);
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      validate();
      return;
    }

    // Step2: Load site specific properties file if not in test mode. Note that we decide whether in
    // test mode by default properties and system properties (via getBoolean).
    Properties siteProps;
    // we are not in test mode, load site properties
    String confPaths = Configuration.get(PropertyKey.SITE_CONF_DIR);
    String[] confPathList = confPaths.split(",");
    String sitePropertyFile =
        ConfigurationUtils.searchPropertiesFile(Constants.SITE_PROPERTIES, confPathList);
    if (sitePropertyFile != null) {
      siteProps = ConfigurationUtils.loadPropertiesFromFile(sitePropertyFile);
    } else {
      siteProps = ConfigurationUtils.loadPropertiesFromResource(Constants.SITE_PROPERTIES);
    }
    PROPERTIES.merge(siteProps, Source.siteProperty(sitePropertyFile));
    validate();
  }

  /**
   * Merges the current configuration properties with new properties. If a property exists
   * both in the new and current configuration, the one from the new configuration wins if
   * its priority is higher or equal than the existing one.
   *
   * @param properties the source {@link Properties} to be merged
   * @param source the source of the the properties (e.g., system property, default and etc)
   */
  public static void merge(Map<?, ?> properties, Source source) {
    PROPERTIES.merge(properties, source);
  }

  // Public accessor methods
  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   */
  public static void set(PropertyKey key, Object value) {
    set(key, String.valueOf(value), Source.RUNTIME);
  }

  /**
   * Sets the value for the appropriate key in the {@link Properties} by source.
   *
   * @param key the key to set
   * @param value the value for the key
   * @param source the source of the the properties (e.g., system property, default and etc)
   */
  public static void set(PropertyKey key, Object value, Source source) {
    Preconditions.checkArgument(key != null && value != null,
        String.format("the key value pair (%s, %s) cannot have null", key, value));
    PROPERTIES.put(key, String.valueOf(value), source);
  }

  /**
   * Unsets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to unset
   */
  public static void unset(PropertyKey key) {
    Preconditions.checkNotNull(key, "key");
    PROPERTIES.remove(key);
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  public static String get(PropertyKey key) {
    return CONF.get(key);
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @param options options for getting configuration value
   * @return the value for the given key
   */
  public static String get(PropertyKey key, ConfigurationValueOptions options) {
    return CONF.get(key, options);
  }

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @return the value
   */
  public static String getOrDefault(PropertyKey key, String defaultValue) {
    return CONF.getOrDefault(key, defaultValue);
  }

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @param options options for getting configuration value
   * @return the value
   */
  public static String getOrDefault(PropertyKey key, String defaultValue,
      ConfigurationValueOptions options) {
    return CONF.getOrDefault(key, defaultValue, options);
  }

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   * @deprecated due to misleading method name, use {{@link #isSet(PropertyKey)}} instead
   */
  @Deprecated
  public static boolean containsKey(PropertyKey key) {
    return isSet(key);
  }

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public static boolean isSet(PropertyKey key) {
    return CONF.isSet(key);
  }

  /**
   * @return the keys configured by the configuration
   */
  public static Set<PropertyKey> keySet() {
    return CONF.keySet();
  }

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  public static int getInt(PropertyKey key) {
    return CONF.getInt(key);
  }

  /**
   * Gets the long representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code long}
   */
  public static long getLong(PropertyKey key) {
    return CONF.getLong(key);
  }

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  public static double getDouble(PropertyKey key) {
    return CONF.getDouble(key);
  }

  /**
   * Gets the float representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code float}
   */
  public static float getFloat(PropertyKey key) {
    return CONF.getFloat(key);
  }

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  public static boolean getBoolean(PropertyKey key) {
    return CONF.getBoolean(key);
  }

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @param delimiter the delimiter to split the values
   * @return the list of values for the given key
   */
  public static List<String> getList(PropertyKey key, String delimiter) {
    return CONF.getList(key, delimiter);
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
    return CONF.getEnum(key, enumType);
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  public static long getBytes(PropertyKey key) {
    return CONF.getBytes(key);
  }

  /**
   * Gets the time of key in millisecond unit.
   *
   * @param key the key to get the value for
   * @return the time of key in millisecond unit
   */
  public static long getMs(PropertyKey key) {
    return CONF.getMs(key);
  }

  /**
   * Gets the time of the key as a duration.
   *
   * @param key the key to get the value for
   * @return the value of the key represented as a duration
   */
  public static Duration getDuration(PropertyKey key) {
    return CONF.getDuration(key);
  }

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  public static <T> Class<T> getClass(PropertyKey key) {
    return CONF.getClass(key);
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
    return CONF.getNestedProperties(prefixKey);
  }

  /**
   * @param key the property key
   * @return the source for the given key
   */
  public static Source getSource(PropertyKey key) {
    return CONF.getSource(key);
  }

  /**
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  public static Map<String, String> toMap() {
    return CONF.toMap();
  }

  /**
   * @param opts options for formatting the configuration values
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  public static Map<String, String> toMap(ConfigurationValueOptions opts) {
    return CONF.toMap(opts);
  }

  /**
   * Validates the configuration.
   *
   * @throws IllegalStateException if invalid configuration is encountered
   */
  public static void validate() {
    CONF.validate();
  }

  /**
   * @return the {@link InstancedConfiguration} object backing the global configuration
   */
  public static InstancedConfiguration global() {
    return CONF;
  }

  private Configuration() {} // prevent instantiation
}
