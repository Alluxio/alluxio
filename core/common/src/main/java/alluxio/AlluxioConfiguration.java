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

import alluxio.PropertyKey.Template;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.Source;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.util.FormatUtils;
import alluxio.wire.ConfigProperty;
import alluxio.wire.Scope;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

public class AlluxioConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

  /** Regex string to find "${key}" for variable substitution. */
  private static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  /** Regex to find ${key} for variable substitution. */
  private static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);
  /** Source of the truth of all property values (default or customized). */
  private final AlluxioProperties mProperties;

  public AlluxioConfiguration(AlluxioProperties properties) {
    mProperties = properties;
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  public String get(PropertyKey key) {
    String rawValue = mProperties.get(key);
    if (rawValue == null) {
      // if key is not found among the default properties
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }
    return lookup(rawValue);
  }

  /**
   * Checks if the configuration contains value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public boolean containsKey(PropertyKey key) {
    return mProperties.hasValueSet(key);
  }

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  public int getInt(PropertyKey key) {
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
  public long getLong(PropertyKey key) {
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
  public double getDouble(PropertyKey key) {
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
  public float getFloat(PropertyKey key) {
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
  public boolean getBoolean(PropertyKey key) {
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
  public List<String> getList(PropertyKey key, String delimiter) {
    Preconditions.checkArgument(delimiter != null,
        "Illegal separator for Alluxio properties as list");
    String rawValue = get(key);

    return Lists.newArrayList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
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
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    String rawValue = get(key);

    try {
      return Enum.valueOf(enumType, rawValue);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(ExceptionMessage.UNKNOWN_ENUM.getMessage(rawValue,
          Arrays.toString(enumType.getEnumConstants())));
    }
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  public long getBytes(PropertyKey key) {
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
  public long getMs(PropertyKey key) {
    String rawValue = get(key);
    try {
      return FormatUtils.parseTimeSize(rawValue);
    } catch (Exception e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_MS.getMessage(key));
    }
  }

  /**
   * Gets the time of the key as a duration.
   *
   * @param key the key to get the value for
   * @return the value of the key represented as a duration
   */
  public Duration getDuration(PropertyKey key) {
    return Duration.ofMillis(getMs(key));
  }

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  public <T> Class<T> getClass(PropertyKey key) {
    String rawValue = get(key);

    try {
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(rawValue);
      return clazz;
    } catch (Exception e) {
      LOG.error("requested class could not be loaded: {}", rawValue, e);
      throw new RuntimeException(e);
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
  public Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    Map<String, String> ret = Maps.newHashMap();
    for (Map.Entry<PropertyKey, String> entry: mProperties.entrySet()) {
      String key = entry.getKey().getName();
      if (prefixKey.isNested(key)) {
        String suffixKey = key.substring(prefixKey.length() + 1);
        ret.put(suffixKey, entry.getValue());
      }
    }
    return ret;
  }

  /**
   * @return a view of the resolved properties represented by this configuration,
   *         including all default properties
   */
  public Map<String, String> toMap() {
    Map<String, String> map = toRawMap();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String value = entry.getValue();
      if (value != null) {
        map.put(entry.getKey(), lookup(value));
      } else {
        map.put(entry.getKey(), value);
      }
    }
    return map;
  }

  /**
   * @return a map of the raw properties represented by this configuration,
   *         including all default properties
   */
  public Map<String, String> toRawMap() {
    Map<String, String> rawMap = new HashMap<>();
    mProperties.forEach((key, value) -> rawMap.put(key.getName(), value));
    return rawMap;
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base the String to look for
   * @return resolved String value
   */
  private String lookup(final String base) {
    return lookupRecursively(base, new HashSet<>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the string to resolve
   * @param seen strings already seen during this lookup, used to prevent unbound recursion
   * @return the resolved string
   */
  @Nullable
  private String lookupRecursively(String base, Set<String> seen) {
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
      if (!seen.add(match)) {
        throw new RuntimeException("Circular dependency found while resolving " + match);
      }
      if (!PropertyKey.isValid(match)) {
        throw new RuntimeException("Invalid property key " + match);
      }
      String value = lookupRecursively(mProperties.get(PropertyKey.fromString(match)), seen);
      seen.remove(match);
      if (value == null) {
        throw new RuntimeException("No value specified for configuration property " + match);
      }
      LOG.debug("Replacing {} with {}", matcher.group(1), value);
      resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(value));
    }
    return resolved;
  }

  /**
   * @param key the property key
   * @return the source for the given key
   */
  public Source getSource(PropertyKey key) {
    return mProperties.getSource(key);
  }

  /**
   * Validates worker port configuration.
   *
   * @throws IllegalStateException if invalid worker port configuration is encountered
   */
  private void checkWorkerPorts() {
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
    }
  }

  /**
   * Validates timeout related configuration.
   */
  private void checkTimeouts() {
    long waitTime = getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    long retryInterval = getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
    if (waitTime < retryInterval) {
      LOG.warn("{}={}ms is smaller than {}={}ms. Workers might not have enough time to register. "
              + "Consider either increasing {} or decreasing {}",
          PropertyKey.Name.MASTER_WORKER_CONNECT_WAIT_TIME, waitTime,
          PropertyKey.Name.USER_RPC_RETRY_MAX_SLEEP_MS, retryInterval,
          PropertyKey.Name.MASTER_WORKER_CONNECT_WAIT_TIME,
          PropertyKey.Name.USER_RPC_RETRY_MAX_SLEEP_MS);
    }
  }

  /**
   * Validates the user file buffer size is a non-negative number.
   *
   * @throws IllegalStateException if invalid user file buffer size configuration is encountered
   */
  private void checkUserFileBufferBytes() {
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
  private void checkZkConfiguration() {
    Preconditions.checkState(
        containsKey(PropertyKey.ZOOKEEPER_ADDRESS) == getBoolean(PropertyKey.ZOOKEEPER_ENABLED),
        PreconditionMessage.INCONSISTENT_ZK_CONFIGURATION.toString(),
        PropertyKey.Name.ZOOKEEPER_ADDRESS, PropertyKey.Name.ZOOKEEPER_ENABLED);
  }

  /**
   * Checks that tiered locality configuration is consistent.
   *
   * @throws IllegalStateException if invalid tiered locality configuration is encountered
   */
  private void checkTieredLocality() {
    // Check that any custom tiers set by alluxio.locality.{custom_tier}=value are also defined in
    // the tier ordering defined by alluxio.locality.order.
    Set<String> tiers = Sets.newHashSet(getList(PropertyKey.LOCALITY_ORDER, ","));
    Set<PropertyKey> predefinedKeys = new HashSet<>(PropertyKey.defaultKeys());
    for (PropertyKey key : mProperties.keySet()) {
      if (predefinedKeys.contains(key)) {
        // Skip non-templated keys.
        continue;
      }
      Matcher matcher = Template.LOCALITY_TIER.match(key.toString());
      if (matcher.matches() && matcher.group(1) != null) {
        String tierName = matcher.group(1);
        if (!tiers.contains(tierName)) {
          throw new IllegalStateException(
              String.format("Tier %s is configured by %s, but does not exist in the tier list %s "
                  + "configured by %s", tierName, key, tiers, PropertyKey.LOCALITY_ORDER));
        }
      }
    }
  }

  /**
   * Validates the configuration.
   *
   * @throws IllegalStateException if invalid configuration is encountered
   */
  public void validate() {
    for (Map.Entry<String, String> entry : toMap().entrySet()) {
      String propertyName = entry.getKey();
      Preconditions.checkState(PropertyKey.isValid(propertyName), propertyName);
      PropertyKey propertyKey = PropertyKey.fromString(propertyName);
      Preconditions.checkState(
          getSource(propertyKey).getType() != Source.Type.SITE_PROPERTY
              || !propertyKey.isIgnoredSiteProperty(),
          "%s is not accepted in alluxio-site.properties, "
              + "and must be specified as a JVM property. "
              + "If no JVM property is present, Alluxio will use default value '%s'.", propertyName,
          propertyKey.getDefaultValue());
    }
    checkTimeouts();
    checkWorkerPorts();
    checkUserFileBufferBytes();
    checkZkConfiguration();
    checkTieredLocality();
  }

  /**
   * Gets the raw configuration of a given scope.
   *
   * @param scope the property key scope
   * @return a list of raw configurations inside the property scope
   */
  public List<ConfigProperty> getConfiguration(Scope scope) {
    List<ConfigProperty> list = new ArrayList<>();
    for (Map.Entry<String, String> entry : toRawMap().entrySet()) {
      PropertyKey key = PropertyKey.fromString(entry.getKey());
      if (key.getScope().contains(scope) && containsKey(key)) {
        ConfigProperty configProperty = new ConfigProperty().setName(key.getName())
            .setValue(get(key)).setSource(Configuration.getSource(key).toString());
        list.add(configProperty);
      }
    }
    return list;
  }
}
