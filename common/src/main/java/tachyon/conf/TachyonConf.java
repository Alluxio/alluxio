/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.exception.ExceptionMessage;
import tachyon.network.ChannelType;
import tachyon.util.FormatUtils;
import tachyon.util.network.NetworkAddressUtils;

/**
 * <p>
 * All the runtime configuration properties of Tachyon. This class works like a dictionary and
 * serves each Tachyon configuration property as a key-value pair.
 *
 * <p>
 * Tachyon configuration properties are loaded into this class in the following order with
 * decreasing priority:
 * <ol>
 * <li>Java system properties;</li>
 * <li>Environment variables via {@code tachyon-env.sh} or from OS settings;</li>
 * <li>Site specific properties via {@code tachyon-site.properties} file;</li>
 * <li>Default properties via {@code tachyon-default.properties} file.</li>
 * </ol>
 *
 * <p>
 * The default properties are defined in a property file {@code tachyon-default.properties}
 * distributed with Tachyon jar. Tachyon users can override values of these default properties by
 * creating {@code tachyon-site.properties} and putting it under java {@code CLASSPATH} when
 * running Tachyon (e.g., ${TACHYON_HOME}/conf/)
 *
 * <p>
 * Developers can create an instance of this class by {@code new TachyonConf()}, which will load
 * values from any Java system properties set as well.
 *
 * <p>
 * The class only supports creation using {@code new TachyonConf(properties)} to override default
 * values.
 */
public final class TachyonConf {
  /** File to set default properties */
  public static final String DEFAULT_PROPERTIES = "tachyon-default.properties";
  /** File to set customized properties */
  public static final String SITE_PROPERTIES = "tachyon-site.properties";
  /** Regex string to find ${key} for variable substitution */
  private static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  /** Regex to find ${key} for variable substitution */
  private static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Properties mProperties = new Properties();

  /**
   * Overrides default properties.
   *
   * @param props override {@link Properties}
   */
  public TachyonConf(Map<String, String> props) {
    if (props != null) {
      mProperties.putAll(props);
    }
    checkUserFileBufferBytes();
  }

  /**
   * Overrides default properties.
   *
   * @param props override {@link Properties}
   */
  public TachyonConf(Properties props) {
    if (props != null) {
      mProperties.putAll(props);
    }
    checkUserFileBufferBytes();
  }

  /**
   * Default constructor.
   *
   * Most clients will call this constructor to allow default loading of properties to happen.
   */
  public TachyonConf() {
    this(true);
  }

  /**
   * Constructor with a flag to indicate whether system properties should be included. When the flag
   * is set to false, it is used for TachyonConfTest class.
   *
   * @param includeSystemProperties whether to include the system properties
   */
  TachyonConf(boolean includeSystemProperties) {
    // Load default
    Properties defaultProps = new Properties();

    // Override runtime default
    defaultProps.setProperty(Constants.MASTER_HOSTNAME, NetworkAddressUtils.getLocalHostName(250));
    defaultProps.setProperty(Constants.WORKER_WORKER_BLOCK_THREADS_MIN,
        String.valueOf(Runtime.getRuntime().availableProcessors()));
    defaultProps.setProperty(Constants.MASTER_WORKER_THREADS_MIN,
        String.valueOf(Runtime.getRuntime().availableProcessors()));
    defaultProps.setProperty(Constants.WORKER_NETWORK_NETTY_CHANNEL,
        String.valueOf(ChannelType.defaultType()));
    defaultProps.setProperty(Constants.USER_NETWORK_NETTY_CHANNEL,
        String.valueOf(ChannelType.defaultType()));

    InputStream defaultInputStream =
        TachyonConf.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES);
    if (defaultInputStream == null) {
      throw new RuntimeException(ExceptionMessage.DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST
              .getMessage());
    }
    try {
      defaultProps.load(defaultInputStream);
    } catch (IOException e) {
      throw new RuntimeException(ExceptionMessage.UNABLE_TO_LOAD_PROPERTIES_FILE.getMessage(), e);
    }

    // Load site specific properties file
    Properties siteProps = new Properties();
    InputStream siteInputStream =
        TachyonConf.class.getClassLoader().getResourceAsStream(SITE_PROPERTIES);
    if (siteInputStream != null) {
      try {
        siteProps.load(siteInputStream);
      } catch (IOException e) {
        LOG.warn("Unable to load site Tachyon configuration file.", e);
      }
    }

    // Load system properties
    Properties systemProps = new Properties();
    if (includeSystemProperties) {
      systemProps.putAll(System.getProperties());
    }

    // Now lets combine
    mProperties.putAll(defaultProps);
    mProperties.putAll(siteProps);
    mProperties.putAll(systemProps);

    // Update tachyon.master_address based on if Zookeeper is used or not.
    String masterHostname = mProperties.getProperty(Constants.MASTER_HOSTNAME);
    String masterPort = mProperties.getProperty(Constants.MASTER_RPC_PORT);
    boolean useZk = Boolean.parseBoolean(mProperties.getProperty(Constants.ZOOKEEPER_ENABLED));
    String masterAddress =
        (useZk ? Constants.HEADER_FT : Constants.HEADER) + masterHostname + ":" + masterPort;
    mProperties.setProperty(Constants.MASTER_ADDRESS, masterAddress);
    checkUserFileBufferBytes();

    // Make sure the user hasn't set worker ports when there may be multiple workers per host
    int maxWorkersPerHost = getInt(Constants.INTEGRATION_YARN_WORKERS_PER_HOST_MAX);
    if (maxWorkersPerHost > 1) {
      String message = "%s cannot be specified when allowing multiple workers per host with "
          + Constants.INTEGRATION_YARN_WORKERS_PER_HOST_MAX + "=" + maxWorkersPerHost;
      Preconditions.checkState(System.getProperty(Constants.WORKER_DATA_PORT) == null,
          String.format(message, Constants.WORKER_DATA_PORT));
      Preconditions.checkState(System.getProperty(Constants.WORKER_RPC_PORT) == null,
          String.format(message, Constants.WORKER_RPC_PORT));
      Preconditions.checkState(System.getProperty(Constants.WORKER_WEB_PORT) == null,
          String.format(message, Constants.WORKER_WEB_PORT));
      mProperties.setProperty(Constants.WORKER_DATA_PORT, "0");
      mProperties.setProperty(Constants.WORKER_RPC_PORT, "0");
      mProperties.setProperty(Constants.WORKER_WEB_PORT, "0");
    }
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (Object s : mProperties.keySet()) {
      hash ^= s.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TachyonConf)) {
      return false;
    }
    TachyonConf that = (TachyonConf) o;
    return mProperties.equals(that.mProperties);
  }

  /**
   * @return the deep copy of the internal {@link Properties} of this {@link TachyonConf} instance
   */
  public Properties getInternalProperties() {
    return SerializationUtils.clone(mProperties);
  }

  /**
   * Merge the current configuration properties with another one. A property from the new
   * configuration wins if it also appears in the current configuration.
   *
   * @param alternateConf The source {@link TachyonConf} to be merged
   */
  public void merge(TachyonConf alternateConf) {
    if (alternateConf != null) {
      // merge the system properties
      mProperties.putAll(alternateConf.getInternalProperties());
    }
  }

  // Public accessor methods

  // TODO(binfan): this method should be hidden and only used during initialization and tests.

  /**
   * Sets the value for the appropriate key in the {@link Properties}.
   *
   * @param key the key to set
   * @param value the value for the key
   */
  public void set(String key, String value) {
    Preconditions.checkArgument(key != null && value != null,
        String.format("the key value pair (%s, %s) cannot have null", key, value));
    mProperties.put(key, value);
  }

  /**
   * Gets the value for the given key in the {@link Properties}.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  public String get(String key) {
    if (!mProperties.containsKey(key)) {
      // if key is not found among the default properties
      throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
    }
    String raw = mProperties.getProperty(key);
    return lookup(raw);
  }

  /**
   * Checks if the {@link Properties} contains the given key.
   *
   * @param key the key to check
   * @return true if the key is in the {@link Properties}, false otherwise
   */
  public boolean containsKey(String key) {
    return mProperties.containsKey(key);
  }

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  public int getInt(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Integer.parseInt(lookup(rawValue));
      } catch (NumberFormatException e) {
        throw new RuntimeException(ExceptionMessage.KEY_NOT_INTEGER.getMessage(key));
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the long representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code long}
   */
  public long getLong(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Long.parseLong(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key {} as long.", key);
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  public double getDouble(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Double.parseDouble(lookup(rawValue));
      } catch (NumberFormatException e) {
        throw new RuntimeException(ExceptionMessage.KEY_NOT_DOUBLE.getMessage(key));
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the float representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code float}
   */
  public float getFloat(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Float.parseFloat(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key {} as float.", key);
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  public boolean getBoolean(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Boolean.parseBoolean(lookup(rawValue));
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @param delimiter the delimiter to split the values
   * @return the list of values for the given key
   */
  public List<String> getList(String key, String delimiter) {
    Preconditions.checkArgument(delimiter != null, "Illegal separator for Tachyon properties as "
        + "list");
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Lists.newLinkedList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
          .split(rawValue));
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the value for the given key as an enum value.
   *
   * @param key the key to get the value for
   * @param enumType the type of the enum
   * @param <T> the type of the enum
   * @return the value for the given key as an enum value
   */
  public <T extends Enum<T>> T getEnum(String key, Class<T> enumType) {
    if (!mProperties.containsKey(key)) {
      throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
    }
    final String val = get(key);
    return Enum.valueOf(enumType, val);
  }

  /**
   * Gets the bytes of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the bytes of the value for the given key
   */
  public long getBytes(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = get(key);
      try {
        return FormatUtils.parseSpaceSize(rawValue);
      } catch (Exception ex) {
        throw new RuntimeException(ExceptionMessage.KEY_NOT_BYTES.getMessage(key));
      }
    }
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getClass(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return (Class<T>) Class.forName(rawValue);
      } catch (Exception e) {
        String msg = "requested class could not be loaded";
        LOG.error("{} : {} , {}", msg, rawValue, e);
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException(ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
  }

  /**
   * Returns the properties as a Map.
   *
   * @return a Map from each property name to its property values
   */
  public Map<String, String> toMap() {
    Map<String, String> copy = new HashMap<String, String>();
    for (Enumeration<?> names = mProperties.propertyNames(); names.hasMoreElements();) {
      Object key = names.nextElement();
      copy.put(key.toString(), mProperties.get(key).toString());
    }
    return copy;
  }

  @Override
  public String toString() {
    return mProperties.toString();
  }

  /**
   * Lookup key names to handle ${key} stuff. Set as package private for testing.
   *
   * @param base string to look for
   * @return the key name with the ${key} substituted
   */
  private String lookup(String base) {
    return lookupRecursively(base, new HashMap<String, String>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the String to look for
   * @param found {@link Map} of String that already seen in this path
   * @return resolved String value
   */
  private String lookupRecursively(final String base, Map<String, String> found) {
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
        value = lookupRecursively(mProperties.getProperty(match), found);
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
   * {@link Constants#USER_FILE_BUFFER_BYTES} should not bigger than {@link Integer#MAX_VALUE}
   * bytes.
   *
   * @throws IllegalArgumentException if USER_FILE_BUFFER_BYTES bigger than Integer.MAX_VALUE
   */
  private void checkUserFileBufferBytes() {
    if (!containsKey(Constants.USER_FILE_BUFFER_BYTES)) { //load from hadoop conf
      return;
    }
    long usrFileBufferBytes = getBytes(Constants.USER_FILE_BUFFER_BYTES);
    Preconditions.checkArgument((usrFileBufferBytes & Integer.MAX_VALUE) == usrFileBufferBytes,
        "Invalid \"" + Constants.USER_FILE_BUFFER_BYTES + "\": " + usrFileBufferBytes);
  }
}
