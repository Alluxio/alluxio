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
import alluxio.wire.ConfigProperty;
import alluxio.wire.Scope;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    Preconditions.checkArgument(key != null && value != null,
        String.format("the key value pair (%s, %s) cannot have null", key, value));
    PROPERTIES.put(key, String.valueOf(value), Source.RUNTIME);
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
<<<<<<< HEAD
    return get(key, ValueOptions.defaults().checkNullValue(true));
  }

  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @param options options for getting configuration value
   * @return the value for the given key
   */
  public static String get(PropertyKey key, ValueOptions options) {
    String value = PROPERTIES.get(key);
    if (options.shouldCheckNullValue() && value == null) {
      // if key is not found among the default properties
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
||||||| merged common ancestors
    String rawValue = PROPERTIES.get(key);
    if (rawValue == null) {
      // if key is not found among the default properties
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }
    return lookup(rawValue);
=======
    return CONF.get(key);
>>>>>>> master
  }

  /**
   * Checks if the configuration contains value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public static boolean containsKey(PropertyKey key) {
    return CONF.containsKey(key);
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
   * Options for getting configuration values.
   */
  public static final class ValueOptions {
    private boolean mUseDisplayValue;
    private boolean mUseRawValue;
    private boolean mCheckNullValue;

    /**
     * @return the default {@link ValueOptions}
     */
    public static ValueOptions defaults() {
      return new ValueOptions();
    }

    private ValueOptions() {
      // prevents instantiation
    }

    /**
     * @return whether to check null value
     */
    public boolean shouldCheckNullValue() {
      return mCheckNullValue;
    }

    /**
     * @return whether to use display value
     */
    public boolean shouldUseDisplayValue() {
      return mUseDisplayValue;
    }

    /**
     * @return whether to use raw value
     */
    public boolean shouldUseRawValue() {
      return mUseRawValue;
    }

    /**
     * @param checkNullValue whether to check null value
     * @return the {@link ValueOptions} instance
     */
    public ValueOptions checkNullValue(boolean checkNullValue) {
      mCheckNullValue = checkNullValue;
      return this;
    }

    /**
     * @param useRawValue whether to use raw value
     * @return the {@link ValueOptions} instance
     */
    public ValueOptions useRawValue(boolean useRawValue) {
      mUseRawValue = useRawValue;
      return this;
    }

    /**
     * @param useDisplayValue whether to use display value
     * @return the {@link ValueOptions} instance
     */
    public ValueOptions useDisplayValue(boolean useDisplayValue) {
      mUseDisplayValue = useDisplayValue;
      return this;
    }
  }

  /**
   * @param options option for getting configuration values
   * @return a view of the properties represented by this configuration,
   *         including all default properties
   */
  public static Map<String, String> toMap(ValueOptions options) {
    Map<String, String> map = new HashMap<>();
    PROPERTIES.forEach((key, value) ->
        map.put(key.getName(), get(key, options)));
    return map;
  }

  /**
   * @return a view of the resolved properties represented by this configuration,
   *         including all default properties
   */
  public static Map<String, String> toMap() {
<<<<<<< HEAD
    return toMap(ValueOptions.defaults());
||||||| merged common ancestors
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
=======
    return CONF.toMap();
>>>>>>> master
  }

  /**
   * @return a map of the raw properties represented by this configuration,
   *         including all default properties
   */
  public static Map<String, String> toRawMap() {
<<<<<<< HEAD
    return toMap(ValueOptions.defaults().useRawValue(true));
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base the String to look for
   * @return resolved String value
   */
  private static String lookup(final String base) {
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
  private static String lookupRecursively(String base, Set<String> seen) {
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
      String value = lookupRecursively(PROPERTIES.get(PropertyKey.fromString(match)), seen);
      seen.remove(match);
      if (value == null) {
        throw new RuntimeException("No value specified for configuration property " + match);
      }
      LOG.debug("Replacing {} with {}", matcher.group(1), value);
      resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(value));
    }
    return resolved;
||||||| merged common ancestors
    Map<String, String> rawMap = new HashMap<>();
    PROPERTIES.forEach((key, value) -> rawMap.put(key.getName(), value));
    return rawMap;
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base the String to look for
   * @return resolved String value
   */
  private static String lookup(final String base) {
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
  private static String lookupRecursively(String base, Set<String> seen) {
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
      String value = lookupRecursively(PROPERTIES.get(PropertyKey.fromString(match)), seen);
      seen.remove(match);
      if (value == null) {
        throw new RuntimeException("No value specified for configuration property " + match);
      }
      LOG.debug("Replacing {} with {}", matcher.group(1), value);
      resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(value));
    }
    return resolved;
=======
    return CONF.toRawMap();
>>>>>>> master
  }

  /**
   * @param key the property key
   * @return the source for the given key
   */
  public static Source getSource(PropertyKey key) {
    return CONF.getSource(key);
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
   * Gets the raw (no alias lookup) display configuration of a given scope.
   *
   * @param scope the property key scope
   * @return a list of raw configurations inside the property scope
   */
  public static List<ConfigProperty> getConfiguration(Scope scope) {
<<<<<<< HEAD
    List<ConfigProperty> list = new ArrayList<>();
    for (Map.Entry<String, String> entry : toMap(ValueOptions.defaults()
        .useRawValue(true).useDisplayValue(true)).entrySet()) {
      PropertyKey key = PropertyKey.fromString(entry.getKey());
      if (key.getScope().contains(scope) && containsKey(key)) {
        ConfigProperty configProperty = new ConfigProperty()
            .setName(key.getName()).setValue(get(key)).setSource(getFormattedSource(key));
        list.add(configProperty);
      }
    }
    return list;
||||||| merged common ancestors
    List<ConfigProperty> list = new ArrayList<>();
    for (Map.Entry<String, String> entry : toRawMap().entrySet()) {
      PropertyKey key = PropertyKey.fromString(entry.getKey());
      if (key.getScope().contains(scope) && containsKey(key)) {
        ConfigProperty configProperty = new ConfigProperty()
            .setName(key.getName()).setValue(get(key)).setSource(getFormattedSource(key));
        list.add(configProperty);
      }
    }
    return list;
=======
    return CONF.getConfiguration(scope);
  }

  /**
   * @return the {@link InstancedConfiguration} object backing the global configuration
   */
  public static InstancedConfiguration global() {
    return CONF;
>>>>>>> master
  }

  private Configuration() {} // prevent instantiation
}
