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

import alluxio.annotation.PublicApi;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Alluxio configuration.
 */
@PublicApi
public interface AlluxioConfiguration {

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  String get(PropertyKey key);

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @param options options for getting configuration value
   * @return the value for the given key
   */
  String get(PropertyKey key, ConfigurationValueOptions options);

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @return the value
   */
  default String getOrDefault(PropertyKey key, String defaultValue) {
    return isSet(key) ? get(key) : defaultValue;
  }

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @param options options for getting configuration value
   * @return the value
   */
  default String getOrDefault(PropertyKey key, String defaultValue,
      ConfigurationValueOptions options) {
    return isSet(key) ? get(key, options) : defaultValue;
  }

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  boolean isSet(PropertyKey key);

  /**
   * @param key the key to check
   * @return true if there is value for the key set by user, false otherwise even when there is a
   *         default value for the key
   */
  boolean isSetByUser(PropertyKey key);

  /**
   * @return the keys configured by the configuration
   */
  Set<PropertyKey> keySet();

  /**
   * @return the keys set by user
   */
  Set<PropertyKey> userKeySet();

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  int getInt(PropertyKey key);

  /**
   * Gets the long representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code long}
   */
  long getLong(PropertyKey key);

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  double getDouble(PropertyKey key);

  /**
   * Gets the float representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code float}
   */
  float getFloat(PropertyKey key);

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  boolean getBoolean(PropertyKey key);

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @param delimiter the delimiter to split the values
   * @return the list of values for the given key
   */
  List<String> getList(PropertyKey key, String delimiter);

  /**
   * Gets the value for the given key as an enum value.
   *
   * @param key the key to get the value for
   * @param enumType the type of the enum
   * @param <T> the type of the enum
   * @return the value for the given key as an enum value
   */
  <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType);

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  long getBytes(PropertyKey key);

  /**
   * Gets the time of key in millisecond unit.
   *
   * @param key the key to get the value for
   * @return the time of key in millisecond unit
   */
  long getMs(PropertyKey key);

  /**
   * Gets the time of the key as a duration.
   *
   * @param key the key to get the value for
   * @return the value of the key represented as a duration
   */
  Duration getDuration(PropertyKey key);

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  <T> Class<T> getClass(PropertyKey key);

  /**
   * Gets a set of properties that share a given common prefix key as a map. E.g., if A.B=V1 and
   * A.C=V2, calling this method with prefixKey=A returns a map of {B=V1, C=V2}, where B and C are
   * also valid properties. If no property shares the prefix, an empty map is returned.
   *
   * @param prefixKey the prefix key
   * @return a map from nested properties aggregated by the prefix
   */
  Map<String, String> getNestedProperties(PropertyKey prefixKey);

  /**
   * Gets a copy of the {@link AlluxioProperties} which back the {@link AlluxioConfiguration}.
   *
   * @return A copy of AlluxioProperties representing the configuration
   */
  AlluxioProperties copyProperties();

  /**
   * @param key the property key
   * @return the source for the given key
   */
  Source getSource(PropertyKey key);

  /**
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  default Map<String, String> toMap() {
    return toMap(ConfigurationValueOptions.defaults());
  }

  /**
   * @param opts options for formatting the configuration values
   * @return a map from all configuration property names to their values; values may potentially be
   *         null
   */
  Map<String, String> toMap(ConfigurationValueOptions opts);

  /**
   * Validates the configuration.
   *
   * @throws IllegalStateException if invalid configuration is encountered
   */
  void validate();

  /**
   * @return whether or not the configuration has been merged with cluster defaults
   */
  boolean clusterDefaultsLoaded();

  /**
   * @return hash of properties, if hashing is not supported, return empty string
   */
  default String hash() {
    return "";
  }
}
