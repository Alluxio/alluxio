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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.Scope;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
public final class Configuration
{
  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

  private static final AtomicReference<InstancedConfiguration> SERVER_CONFIG_REFERENCE =
      new AtomicReference<>();

  static {
    reloadProperties();
  }

  /**
   * Create and return a copy of all properties.
   *
   * @return a copy of properties
   */
  public static AlluxioProperties copyProperties() {
    return new AlluxioProperties(SERVER_CONFIG_REFERENCE.get().copyProperties());
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
    SERVER_CONFIG_REFERENCE.get().merge(properties, source);
  }

  // Public accessor methods
  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   */
  public static void set(PropertyKey key, Object value) {
    set(key, value, Source.RUNTIME);
  }

  /**
   * Sets the value for the appropriate key in the {@link Properties} by source.
   *
   * @param key the key to set
   * @param value the value for the key
   * @param source the source of the the properties (e.g., system property, default and etc)
   */
  public static void set(PropertyKey key, Object value, Source source) {
    if (key.getType() == PropertyKey.PropertyType.STRING) {
      value = String.valueOf(value);
    }
    SERVER_CONFIG_REFERENCE.get().set(key, value, source);
  }

  /**
   * Unsets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to unset
   */
  public static void unset(PropertyKey key) {
    SERVER_CONFIG_REFERENCE.get().unset(key);
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  public static Object get(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().get(key);
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @param options options for getting configuration value
   * @return the value for the given key
   */
  public static Object get(PropertyKey key, ConfigurationValueOptions options) {
    return SERVER_CONFIG_REFERENCE.get().get(key, options);
  }

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @param <T> the type of default value
   * @return the value
   */
  public static <T> T getOrDefault(PropertyKey key, T defaultValue) {
    return SERVER_CONFIG_REFERENCE.get().getOrDefault(key, defaultValue);
  }

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @param options options for getting configuration value
   * @return the value
   */
  public static Object getOrDefault(PropertyKey key, String defaultValue,
      ConfigurationValueOptions options) {
    return SERVER_CONFIG_REFERENCE.get().getOrDefault(key, defaultValue, options);
  }

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public static boolean isSet(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().isSet(key);
  }

  /**
   * Checks if the configuration contains a value for the given key that is set by a user.
   *
   * @param key the key to check
   * @return true if there is value for the key by a user, false otherwise
   */
  public static boolean isSetByUser(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().isSetByUser(key);
  }

  /**
   * @return the keys configured by the configuration
   */
  public static Set<PropertyKey> keySet() {
    return SERVER_CONFIG_REFERENCE.get().keySet();
  }

  /**
   * Gets the String value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code String}
   */
  public static String getString(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getString(key);
  }

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  public static int getInt(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getInt(key);
  }

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  public static double getDouble(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getDouble(key);
  }

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  public static boolean getBoolean(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getBoolean(key);
  }

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @return the list of values for the given key
   */
  public static List<String> getList(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getList(key);
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
    return SERVER_CONFIG_REFERENCE.get().getEnum(key, enumType);
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  public static long getBytes(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getBytes(key);
  }

  /**
   * Gets the time of key in millisecond unit.
   *
   * @param key the key to get the value for
   * @return the time of key in millisecond unit
   */
  public static long getMs(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getMs(key);
  }

  /**
   * Gets the time of the key as a duration.
   *
   * @param key the key to get the value for
   * @return the value of the key represented as a duration
   */
  public static Duration getDuration(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getDuration(key);
  }

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  public static <T> Class<T> getClass(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getClass(key);
  }

  /**
   * Gets a set of properties that share a given common prefix key as a map. E.g., if A.B=V1 and
   * A.C=V2, calling this method with prefixKey=A returns a map of {B=V1, C=V2}, where B and C are
   * also valid properties. If no property shares the prefix, an empty map is returned.
   *
   * @param prefixKey the prefix key
   * @return a map from nested properties aggregated by the prefix
   */
  public static Map<String, Object> getNestedProperties(PropertyKey prefixKey) {
    return SERVER_CONFIG_REFERENCE.get().getNestedProperties(prefixKey);
  }

  /**
   * @param key the property key
   * @return the source for the given key
   */
  public static Source getSource(PropertyKey key) {
    return SERVER_CONFIG_REFERENCE.get().getSource(key);
  }

  /**
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  public static Map<String, Object> toMap() {
    return SERVER_CONFIG_REFERENCE.get().toMap();
  }

  /**
   * @param opts options for formatting the configuration values
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  public static Map<String, Object> toMap(ConfigurationValueOptions opts) {
    return SERVER_CONFIG_REFERENCE.get().toMap(opts);
  }

  /**
   * @return the global configuration through {@link AlluxioConfiguration} API,
   * which is a read-only API
   */
  public static AlluxioConfiguration global() {
    return SERVER_CONFIG_REFERENCE.get();
  }

  /**
   * @return the global configuration instance that is modifiable
   */
  public static InstancedConfiguration modifiableGlobal() {
    return SERVER_CONFIG_REFERENCE.get();
  }

  /**
   * @return a copy of {@link InstancedConfiguration} object based on the global configuration
   */
  public static InstancedConfiguration copyGlobal() {
    return new InstancedConfiguration(SERVER_CONFIG_REFERENCE.get().copyProperties());
  }

  /**
   * Loads cluster default values for workers from the meta master if it's not loaded yet.
   *
   * @param address the master address
   * @param scope the property scope
   */
  public static void loadClusterDefaults(InetSocketAddress address, Scope scope)
      throws AlluxioStatusException {
    InstancedConfiguration conf = SERVER_CONFIG_REFERENCE.get();
    InstancedConfiguration newConf;
    if (conf.getBoolean(PropertyKey.USER_CONF_CLUSTER_DEFAULT_ENABLED)
        && !conf.clusterDefaultsLoaded()) {
      do {
        conf = SERVER_CONFIG_REFERENCE.get();
        GetConfigurationPResponse response = ConfigurationUtils
            .loadConfiguration(address, conf, false, true);
        newConf = ConfigurationUtils.getClusterConf(response, conf, scope);
      } while (!SERVER_CONFIG_REFERENCE.compareAndSet(conf, newConf));
    }
  }

  /**
   * @return hash of properties
   */
  public static String hash() {
    return SERVER_CONFIG_REFERENCE.get().hash();
  }

  private Configuration() {} // prevent instantiation

  /**
   * Reloads site properties from disk.
   */
  public static void reloadProperties() {
    // Bootstrap the configuration. This is necessary because we need to resolve alluxio.home
    // (likely to be in system properties) to locate the conf dir to search for the site
    // property file.
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    // Can't directly pass System.getProperties() because it is not thread-safe
    // This can cause a ConcurrentModificationException when merging.
    alluxioProperties.merge(System.getProperties().entrySet().stream()
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
        Source.SYSTEM_PROPERTY);
    InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);
    // Load site specific properties file if not in test mode. Note that we decide
    // whether in test mode by default properties and system properties (via getBoolean).
    if (!conf.getBoolean(PropertyKey.TEST_MODE)) {
      // We are not in test mode, load site properties
      // First try loading from config file
      for (String path : conf.getList(PropertyKey.SITE_CONF_DIR)) {
        String file = PathUtils.concatPath(path, Constants.SITE_PROPERTIES);
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
          Optional<Properties> properties = loadProperties(fileInputStream);
          if (properties.isPresent()) {
            alluxioProperties.merge(properties.get(), Source.siteProperty(file));
            conf = new InstancedConfiguration(alluxioProperties);
            conf.validate();
            SERVER_CONFIG_REFERENCE.set(conf);
            // If a site conf is successfully loaded, stop trying different paths.
            return;
          }
        } catch (FileNotFoundException e) {
          // skip
        } catch (IOException e) {
          LOG.warn("Failed to close property input stream from {}: {}", file, e.toString());
        }
      }

      // Try to load from resource
      URL resource =
          ConfigurationUtils.class.getClassLoader().getResource(Constants.SITE_PROPERTIES);
      if (resource != null) {
        try (InputStream stream = resource.openStream()) {
          Optional<Properties> properties = loadProperties(stream);
          if (properties.isPresent()) {
            alluxioProperties.merge(properties.get(), Source.siteProperty(resource.getPath()));
            conf = new InstancedConfiguration(alluxioProperties);
            conf.validate();
            SERVER_CONFIG_REFERENCE.set(conf);
          }
        } catch (IOException e) {
          LOG.warn("Failed to read properties from {}: {}", resource, e.toString());
        }
      }
    }
    conf.validate();
    SERVER_CONFIG_REFERENCE.set(conf);
  }

  /**
   * @param stream the stream to read properties from
   * @return a properties object populated from the stream
   */
  private static Optional<Properties> loadProperties(InputStream stream) {
    Properties properties = new Properties();
    try {
      properties.load(stream);
    } catch (IOException e) {
      LOG.warn("Unable to load properties: {}", e.toString());
      return Optional.empty();
    }
    return Optional.of(properties);
  }
}
