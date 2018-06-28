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

import alluxio.AlluxioConfiguration;
import alluxio.ConfigurationValueOptions;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Template;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Alluxio configuration.
 */
public class InstancedConfiguration implements AlluxioConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(InstancedConfiguration.class);

  /** Regex string to find "${key}" for variable substitution. */
  private static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  /** Regex to find ${key} for variable substitution. */
  private static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);
  /** Source of the truth of all property values (default or customized). */
  private final AlluxioProperties mProperties;

  /**
   * @param properties alluxio properties underlying this configuration
   */
  public InstancedConfiguration(AlluxioProperties properties) {
    mProperties = properties;
  }

  /**
   * @param conf configuration to copy
   */
  public InstancedConfiguration(InstancedConfiguration conf) {
    mProperties = new AlluxioProperties(conf.mProperties);
  }

  /**
   * @return the properties backing this configuration
   */
  public AlluxioProperties getProperties() {
    return mProperties;
  }

  @Override
  public String get(PropertyKey key) {
    return get(key, ConfigurationValueOptions.defaults());
  }

  @Override
  public String get(PropertyKey key, ConfigurationValueOptions options) {
    String value = mProperties.get(key);
    if (value == null) {
      // if value or default value is not set in configuration for the given key
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }
    if (!options.shouldUseRawValue()) {
      value = lookup(value);
    }
    if (options.shouldUseDisplayValue()) {
      PropertyKey.DisplayType displayType = key.getDisplayType();
      switch (displayType) {
        case DEFAULT:
          break;
        case CREDENTIALS:
          value = "******";
          break;
        default:
          throw new IllegalStateException(String.format("Invalid displayType %s for property %s",
              displayType.name(), key.getName()));
      }
    }
    return value;
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return mProperties.isSet(key);
  }

  @Override
  public Set<PropertyKey> keySet() {
    return mProperties.keySet();
  }

  @Override
  public int getInt(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Integer.parseInt(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_INTEGER.getMessage(key));
    }
  }

  @Override
  public long getLong(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Long.parseLong(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_LONG.getMessage(key));
    }
  }

  @Override
  public double getDouble(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Double.parseDouble(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_DOUBLE.getMessage(key));
    }
  }

  @Override
  public float getFloat(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Float.parseFloat(lookup(rawValue));
    } catch (NumberFormatException e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_FLOAT.getMessage(key));
    }
  }

  @Override
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

  @Override
  public List<String> getList(PropertyKey key, String delimiter) {
    Preconditions.checkArgument(delimiter != null,
        "Illegal separator for Alluxio properties as list");
    String rawValue = get(key);

    return Lists.newArrayList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
        .split(rawValue));
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    String rawValue = get(key);

    try {
      return Enum.valueOf(enumType, rawValue);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(ExceptionMessage.UNKNOWN_ENUM.getMessage(rawValue,
          Arrays.toString(enumType.getEnumConstants())));
    }
  }

  @Override
  public long getBytes(PropertyKey key) {
    String rawValue = get(key);

    try {
      return FormatUtils.parseSpaceSize(rawValue);
    } catch (Exception ex) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_BYTES.getMessage(key));
    }
  }

  @Override
  public long getMs(PropertyKey key) {
    String rawValue = get(key);
    try {
      return FormatUtils.parseTimeSize(rawValue);
    } catch (Exception e) {
      throw new RuntimeException(ExceptionMessage.KEY_NOT_MS.getMessage(key));
    }
  }

  @Override
  public Duration getDuration(PropertyKey key) {
    return Duration.ofMillis(getMs(key));
  }

  @Override
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

  @Override
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

  @Override
  public Source getSource(PropertyKey key) {
    return mProperties.getSource(key);
  }

  @Override
  public void merge(Map<?, ?> properties, Source source) {
    mProperties.merge(properties, source);
  }

  @Override
  public Map<String, String> toMap(ConfigurationValueOptions opts) {
    Map<String, String> map = new HashMap<>();
    // Cannot use Collectors.toMap because we support null keys.
    keySet().forEach(key -> map.put(key.getName(), getOrDefault(key, null, opts)));
    return map;
  }

  @Override
  public void validate() {
    for (PropertyKey key : keySet()) {
      Preconditions.checkState(
          getSource(key).getType() != Source.Type.SITE_PROPERTY || !key.isIgnoredSiteProperty(),
          "%s is not accepted in alluxio-site.properties, "
              + "and must be specified as a JVM property. "
              + "If no JVM property is present, Alluxio will use default value '%s'.",
          key.getName(), key.getDefaultValue());
    }
    checkTimeouts();
    checkWorkerPorts();
    checkUserFileBufferBytes();
    checkZkConfiguration();
    checkTieredLocality();
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
    checkHeartbeatTimeout(PropertyKey.MASTER_MASTER_HEARTBEAT_INTERVAL,
        PropertyKey.MASTER_HEARTBEAT_TIMEOUT);
    // Skip checking block worker heartbeat config because the timeout is master-side while the
    // heartbeat interval is worker-side.
  }

  /**
   * Checks that the interval is shorter than the timeout.
   *
   * @param intervalKey property key for an interval
   * @param timeoutKey property key for a timeout
   */
  private void checkHeartbeatTimeout(PropertyKey intervalKey, PropertyKey timeoutKey) {
    long interval = getMs(intervalKey);
    long timeout = getMs(timeoutKey);
    Preconditions.checkState(interval < timeout,
        "heartbeat interval (%s=%s) must be less than heartbeat timeout (%s=%s)", intervalKey,
        interval, timeoutKey, timeout);
  }

  /**
   * Validates the user file buffer size is a non-negative number.
   *
   * @throws IllegalStateException if invalid user file buffer size configuration is encountered
   */
  private void checkUserFileBufferBytes() {
    if (!isSet(PropertyKey.USER_FILE_BUFFER_BYTES)) { // load from hadoop conf
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
        isSet(PropertyKey.ZOOKEEPER_ADDRESS) == getBoolean(PropertyKey.ZOOKEEPER_ENABLED),
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
}
