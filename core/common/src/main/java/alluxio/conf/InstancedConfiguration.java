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

import static alluxio.conf.PropertyKey.CONF_REGEX;
import static alluxio.conf.PropertyKey.REGEX_STRING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import alluxio.conf.PropertyKey.Template;
import alluxio.exception.ExceptionMessage;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import javax.annotation.Nonnull;

/**
 * Alluxio configuration.

 * WARNING: This API is not intended to be used outside of internal Alluxio code and may be
 * changed or removed in a future minor release.
 *
 * Application code should use APIs {@link Configuration}.
 *
 */
public class InstancedConfiguration implements AlluxioConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(InstancedConfiguration.class);
  /** Source of the truth of all property values (default or customized). */
  protected final AlluxioProperties mProperties;

  private final boolean mClusterDefaultsLoaded;

  /**
   * Creates a new instance of {@link InstancedConfiguration}.
   *
   * WARNING: This API is not intended to be used outside of internal Alluxio code and may be
   * changed or removed in a future minor release.
   *
   * Application code should use {@link Configuration#global()}.
   *
   * @param properties alluxio properties underlying this configuration
   */
  public InstancedConfiguration(AlluxioProperties properties) {
    this(properties, false);
  }

  /**
   * Creates a new instance of {@link InstancedConfiguration}.
   *
   * WARNING: This API is not intended to be used outside of internal Alluxio code and may be
   * changed or removed in a future minor release.
   *
   * Application code should use {@link Configuration#global()}.
   *
   * @param properties alluxio properties underlying this configuration
   * @param clusterDefaultsLoaded Whether or not the properties represent the cluster defaults
   */
  public InstancedConfiguration(AlluxioProperties properties, boolean clusterDefaultsLoaded) {
    mProperties = properties;
    mClusterDefaultsLoaded = clusterDefaultsLoaded;
  }

  /**
   * Return reference to mProperties.
   * @return mProperties
   */
  public AlluxioProperties getProperties() {
    return mProperties;
  }

  @Override
  public AlluxioProperties copyProperties() {
    return mProperties.copy();
  }

  @Override
  public Object get(PropertyKey key) {
    return get(key, ConfigurationValueOptions.defaults());
  }

  @Override
  public Object get(PropertyKey key, ConfigurationValueOptions options) {
    Object value = mProperties.get(key);
    if (value == null) {
      // if value or default value is not set in configuration for the given key
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }

    if (!(value instanceof String)) {
      return value;
    }

    if (!options.shouldUseRawValue()) {
      try {
        value = lookup(key, (String) value);
      } catch (UnresolvablePropertyException e) {
        throw new RuntimeException("Could not resolve key \""
            + key.getName() + "\": " + e.getMessage(), e);
      }
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

  private boolean isResolvable(PropertyKey key) {
    Object value = mProperties.get(key);
    // null values are unresolvable
    if (value == null) {
      return false;
    }
    try {
      // Lookup to resolve any key before simply returning isSet. An exception will be thrown if
      // the key can't be resolved or if a lower level value isn't set.
      if (value instanceof String) {
        lookup(key, (String) value);
      }
    } catch (UnresolvablePropertyException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return mProperties.isSet(key) && isResolvable(key);
  }

  @Override
  public boolean isSetByUser(PropertyKey key) {
    return mProperties.isSetByUser(key) && isResolvable(key);
  }

  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   */
  public void set(PropertyKey key, Object value) {
    set(key, value, Source.RUNTIME);
  }

  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   *
   * @deprecated API to aid property key type transition
   */
  @java.lang.Deprecated
  public void set(@Nonnull PropertyKey key, @Nonnull String value) {
    checkArgument(!value.equals(""),
        "The key \"%s\" cannot be have an empty string as a value. Use "
            + "Configuration.unset to remove a key from the configuration.", key);
    if (key.validateValue(value)) {
      mProperties.put(key, key.formatValue(value), Source.RUNTIME);
    } else {
      if (key.getType() == PropertyKey.PropertyType.STRING) {
        throw new IllegalArgumentException(
            format("Invalid value for property key %s: %s", key, value));
      }
      LOG.warn(format("The value %s for property key %s is invalid. PropertyKey are now typed "
              + "and require values to be properly typed. Invalid PropertyKey values will not be "
              + "accepted in 3.0", value, key));
      mProperties.put(key, key.parseValue(value), Source.RUNTIME);
    }
  }

  /**
   * Sets the value for the appropriate key in the {@link Properties} by source.
   *
   * @param key the key to set
   * @param value the value for the key
   * @param source the source of the the properties (e.g., system property, default and etc)
   */
  public void set(@Nonnull PropertyKey key, @Nonnull Object value, @Nonnull Source source) {
    checkArgument(!value.equals(""),
        "The key \"%s\" cannot be have an empty string as a value. Use "
            + "Configuration.unset to remove a key from the configuration.", key);
    checkArgument(key.validateValue(value),
        "Invalid value for property key %s: %s", key, value);
    value = key.formatValue(value);
    mProperties.put(key, value, source);
  }

  /**
   * Unsets the value for the appropriate key in the {@link Properties}. If the {@link PropertyKey}
   * has a default value, it will still be considered set after executing this method.
   *
   * @param key the key to unset
   */
  public void unset(PropertyKey key) {
    Preconditions.checkNotNull(key, "key");
    mProperties.remove(key);
  }

  /**
   * Merges map of properties into the current alluxio properties.
   *
   * @param properties map of keys to values
   * @param source the source type for these properties
   */
  public void merge(Map<?, ?> properties, Source source) {
    mProperties.merge(properties, source);
  }

  @Override
  public Set<PropertyKey> keySet() {
    return mProperties.keySet();
  }

  @Override
  public Set<PropertyKey> userKeySet() {
    return mProperties.userKeySet();
  }

  @Override
  public String getString(PropertyKey key) {
    if (key.getType() != PropertyKey.PropertyType.STRING) {
      LOG.warn(format("PropertyKey %s's type is %s, please use proper getter method for the type, "
          + "getString will no longer work for non-STRING property types in 3.0",
          key, key.getType()));
    }
    Object value = get(key);
    if (value instanceof String) {
      return (String) value;
    }
    return value.toString();
  }

  @Override
  public int getInt(PropertyKey key)
  {
    checkArgument(key.getType() == PropertyKey.PropertyType.INTEGER);
    return (int) get(key);
  }

  @Override
  public long getLong(PropertyKey key) {
    // Low-precision types int can be implicitly converted to high-precision types long
    // without loss of precision
    checkArgument(key.getType() == PropertyKey.PropertyType.LONG
        || key.getType() == PropertyKey.PropertyType.INTEGER);
    return ((Number) get(key)).longValue();
  }

  @Override
  public double getDouble(PropertyKey key) {
    checkArgument(key.getType() == PropertyKey.PropertyType.DOUBLE);
    return (double) get(key);
  }

  @Override
  public boolean getBoolean(PropertyKey key) {
    checkArgument(key.getType() == PropertyKey.PropertyType.BOOLEAN);
    return (boolean) get(key);
  }

  @Override
  public List<String> getList(PropertyKey key) {
    checkArgument(key.getType() == PropertyKey.PropertyType.LIST);
    String value = (String) get(key);
    return ConfigurationUtils.parseAsList(value, key.getDelimiter());
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    checkArgument(key.getEnumType().equals(enumType), "PropertyKey %s is not of enum type", key);
    return enumType.cast(get(key));
  }

  @Override
  public long getBytes(PropertyKey key) {
    checkArgument(key.getType() == PropertyKey.PropertyType.DATASIZE);
    return FormatUtils.parseSpaceSize((String) get(key));
  }

  @Override
  public long getMs(PropertyKey key) {
    checkArgument(key.getType() == PropertyKey.PropertyType.DURATION);
    return FormatUtils.parseTimeSize((String) get(key));
  }

  @Override
  public Duration getDuration(PropertyKey key) {
    return Duration.ofMillis(getMs(key));
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key) {
    Object value = get(key);
    if (value instanceof Class) {
      return (Class<T>) value;
    }
    try {
      return (Class<T>) Class.forName((String) value);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          format("Requested class %s can not be loaded", value));
    }
  }

  @Override
  public Map<String, Object> getNestedProperties(PropertyKey prefixKey) {
    Map<String, Object> ret = Maps.newHashMap();
    for (Map.Entry<PropertyKey, Object> entry: mProperties.entrySet()) {
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
  public Map<String, Object> toMap(ConfigurationValueOptions opts) {
    Map<String, Object> map = new HashMap<>();
    // Cannot use Collectors.toMap because we support null keys.
    keySet().forEach(key -> map.put(key.getName(), getOrDefault(key, null, opts)));
    return map;
  }

  @Override
  public void validate() {
    if (!getBoolean(PropertyKey.CONF_VALIDATION_ENABLED)) {
      return;
    }
    for (PropertyKey key : keySet()) {
      checkState(
          getSource(key).getType() != Source.Type.SITE_PROPERTY || !key.isIgnoredSiteProperty(),
          "%s is not accepted in alluxio-site.properties, "
              + "and must be specified as a JVM property. "
              + "If no JVM property is present, Alluxio will use default value '%s'.",
          key.getName(), key.getDefaultValue());

      if (PropertyKey.isDeprecated(key) && getSource(key).compareTo(Source.DEFAULT) != 0) {
        LOG.warn("{} is deprecated. Please avoid using this key in the future. {}", key.getName(),
            PropertyKey.getDeprecationMessage(key));
      }
    }

    checkTimeouts();
    checkWorkerPorts();
    checkUserFileBufferBytes();
    checkZkConfiguration();
    checkTieredLocality();
    checkTieredStorage();
  }

  @Override
  public boolean clusterDefaultsLoaded() {
    return mClusterDefaultsLoaded;
  }

  @Override
  public String hash() {
    return mProperties.hash();
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base the String to look for
   * @return resolved String value
   */
  private Object lookup(PropertyKey key, final String base) throws UnresolvablePropertyException {
    return lookupRecursively(key, base, new HashSet<>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the string to resolve
   * @param seen strings already seen during this lookup, used to prevent unbound recursion
   * @return the resolved string
   */
  private Object lookupRecursively(PropertyKey originalKey, String base, Set<String> seen)
      throws UnresolvablePropertyException {
    // check argument
    if (base == null) {
      throw new UnresolvablePropertyException("Can't resolve property with null value");
    }

    String resolved = base;
    Object resolvedValue = null;
    PropertyKey key = null;
    // Lets find pattern match to ${key}.
    // TODO(hsaputra): Consider using Apache Commons StrSubstitutor.
    Matcher matcher = CONF_REGEX.matcher(base);
    while (matcher.find()) {
      String match = matcher.group(2).trim();
      if (!seen.add(match)) {
        throw new RuntimeException(ExceptionMessage.KEY_CIRCULAR_DEPENDENCY.getMessage(match));
      }
      if (!PropertyKey.isValid(match)) {
        throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(match));
      }
      key = PropertyKey.fromString(match);
      Object value = mProperties.get(key);
      String stringValue = null;
      if (value instanceof String) {
        stringValue = String.valueOf(lookupRecursively(key, (String) value , seen));
      }
      else if (value != null) {
        stringValue = String.valueOf(value);
      }
      seen.remove(match);
      if (stringValue == null) {
        throw new UnresolvablePropertyException(ExceptionMessage
            .UNDEFINED_CONFIGURATION_KEY.getMessage(match));
      }
      resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(stringValue));
    }
    if (key != null) {
      resolvedValue = originalKey.parseValue(resolved);
    } else {
      resolvedValue = resolved;
    }
    return resolvedValue;
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
      checkState(System.getProperty(PropertyKey.Name.WORKER_RPC_PORT) == null,
          String.format(message, PropertyKey.WORKER_RPC_PORT));
      checkState(System.getProperty(PropertyKey.Name.WORKER_WEB_PORT) == null,
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
          PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, waitTime,
          PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, retryInterval,
          PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME,
          PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
    }
    checkHeartbeatTimeout(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL,
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
    checkState(interval < timeout,
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
    checkState((usrFileBufferBytes & Integer.MAX_VALUE) == usrFileBufferBytes,
        "Invalid value of %s: %s",
        PropertyKey.Name.USER_FILE_BUFFER_BYTES, usrFileBufferBytes);
  }

  /**
   * Validates Zookeeper-related configuration and prints warnings for possible sources of error.
   *
   * @throws IllegalStateException if invalid Zookeeper configuration is encountered
   */
  private void checkZkConfiguration() {
    checkState(
        isSet(PropertyKey.ZOOKEEPER_ADDRESS) == getBoolean(PropertyKey.ZOOKEEPER_ENABLED),
        "Inconsistent Zookeeper configuration; %s should be set if and only if %s is true",
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
    Set<String> tiers = Sets.newHashSet(getList(PropertyKey.LOCALITY_ORDER));
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
   * Checks that tiered storage configuration on worker is consistent with the global configuration.
   *
   * @throws IllegalStateException if invalid tiered storage configuration is encountered
   */
  @VisibleForTesting
  void checkTieredStorage() {
    int globalTiers = getInt(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS);
    Set<String> globalTierAliasSet = new HashSet<>();
    for (int i = 0; i < globalTiers; i++) {
      globalTierAliasSet.add(
          getString(PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(i)));
    }
    int workerTiers = getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    checkState(workerTiers <= globalTiers,
        "%s tiers on worker (configured by %s), larger than global %s tiers (configured by %s) ",
        workerTiers, PropertyKey.WORKER_TIERED_STORE_LEVELS,
        globalTiers, PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS);
    for (int i = 0; i < workerTiers; i++) {
      PropertyKey key = Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(i);
      String alias = getString(key);
      checkState(globalTierAliasSet.contains(alias),
          "Alias \"%s\" on tier %s on worker (configured by %s) is not found in global tiered "
              + "storage setting: %s",
          alias, i, key, String.join(", ", globalTierAliasSet));
    }
  }

  private class UnresolvablePropertyException extends Exception {

    public UnresolvablePropertyException(String msg) {
      super(msg);
    }
  }
}
