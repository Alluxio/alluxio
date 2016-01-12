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
 * <li>Environment variables via <code>tachyon-env.sh</code> or from OS settings;</li>
 * <li>Site specific properties via <code>tachyon-site.properties</code> file;</li>
 * <li>Default properties via <code>tachyon-default.properties</code> file.</li>
 * </ol>
 *
 * <p>
 * The default properties are defined in a property file <code>tachyon-default.properties</code>
 * distributed with Tachyon jar. Tachyon users can override values of these default properties by
 * creating <code>tachyon-site.properties</code> and putting it under java <code>CLASSPATH</code>
 * when running Tachyon (e.g., ${TACHYON_HOME}/conf/)
 *
 * <p>
 * Developers can create an instance of this class by <code>new TachyonConf()</code>, which will
 * load values from any Java system properties set as well.
 *
 * <p>
 * The class only supports creation using <code>new TachyonConf(properties)</code> to override
 * default values.
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
   * Test constructor for TachyonConfTest class.
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
      throw new RuntimeException("The default Tachyon properties file does not exist.");
    }
    try {
      defaultProps.load(defaultInputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load default Tachyon properties file.", e);
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
    String masterPort = mProperties.getProperty(Constants.MASTER_PORT);
    boolean useZk = Boolean.parseBoolean(mProperties.getProperty(Constants.ZOOKEEPER_ENABLED));
    String masterAddress =
        (useZk ? Constants.HEADER_FT : Constants.HEADER) + masterHostname + ":" + masterPort;
    mProperties.setProperty(Constants.MASTER_ADDRESS, masterAddress);
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TachyonConf) {
      Properties props = ((TachyonConf) obj).getInternalProperties();
      return mProperties.equals(props);
    }
    return false;
  }

  /**
   * @return the deep copy of the internal <code>Properties</code> of this TachyonConf instance
   */
  public Properties getInternalProperties() {
    return SerializationUtils.clone(mProperties);
  }

  /**
   * Merge the current configuration properties with another one. A property from the new
   * configuration wins if it also appears in the current configuration.
   *
   * @param alternateConf The source <code>TachyonConf</code> to be merged.
   */
  public void merge(TachyonConf alternateConf) {
    if (alternateConf != null) {
      // merge the system properties
      mProperties.putAll(alternateConf.getInternalProperties());
    }
  }

  // Public accessor methods

  // TODO(binfan): this method should be hidden and only used during initialization and tests.
  public void set(String key, String value) {
    mProperties.put(key, value);
  }

  public String get(String key) {
    if (!mProperties.containsKey(key)) {
      // if key is not found among the default properties
      throw new RuntimeException("Invalid configuration key " + key + ".");
    }
    String raw = mProperties.getProperty(key);
    return lookup(raw);
  }

  public boolean containsKey(String key) {
    return mProperties.containsKey(key);
  }

  public int getInt(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Integer.parseInt(lookup(rawValue));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Configuration cannot evaluate key " + key + " as integer.");
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public long getLong(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Long.parseLong(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as long.");
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public double getDouble(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Double.parseDouble(lookup(rawValue));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Configuration cannot evaluate key " + key + " as double.");
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public float getFloat(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Float.parseFloat(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as float.");
      }
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public boolean getBoolean(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Boolean.parseBoolean(lookup(rawValue));
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public List<String> getList(String key, String delimiter) {
    Preconditions.checkArgument(delimiter != null, "Illegal separator for Tachyon properties as "
        + "list");
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Lists.newLinkedList(Splitter.on(',').trimResults().omitEmptyStrings().split(rawValue));
    }
    // if key is not found among the default properties
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

  public <T extends Enum<T>> T getEnum(String key, Class<T> enumType) {
    if (!mProperties.containsKey(key)) {
      throw new RuntimeException("Invalid configuration key " + key + ".");
    }
    final String val = get(key);
    return Enum.valueOf(enumType, val);
  }

  public long getBytes(String key) {
    if (mProperties.containsKey(key)) {
      String rawValue = get(key);
      try {
        return FormatUtils.parseSpaceSize(rawValue);
      } catch (Exception ex) {
        throw new RuntimeException("Configuration cannot evaluate key " + key + " as bytes.");
      }
    }
    throw new RuntimeException("Invalid configuration key " + key + ".");
  }

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
    throw new RuntimeException("Invalid configuration key " + key + ".");
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
   * @param base string to look for.
   * @return the key name with the ${key} substituted
   */
  private String lookup(String base) {
    return lookupRecursively(base, new HashMap<String, String>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the String to look for.
   * @param found {@link Map} of String that already seen in this path.
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
}
