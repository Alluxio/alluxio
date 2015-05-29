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
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.netty.ChannelType;

/**
 * Configuration for Tachyon. Used to set various Tachyon parameters as key-value pairs.
 *
 * This class will contains all the runtime configuration properties.
 *
 * Clients of this class can create a TachyonConf object with <code>new TachyonConf()`</code>,
 * which will load values from any Java system properties set as well.
 *
 * The class only support creation using `new TachyonConf(properties)` which will override default
 * values.
 */
public class TachyonConf {
  public static final String DEFAULT_PROPERTIES = "tachyon-default.properties";
  public static final String SITE_PROPERTIES = "tachyon-site.properties";

  // Regex to find ${key} for variable substitution
  public static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  public static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Properties mProperties = new Properties();

  public static void assertValidPort(final int port, TachyonConf tachyonConf) {
    if (!tachyonConf.getBoolean(Constants.IN_TEST_MODE, false)) {
      Preconditions.checkArgument(port > 0, "Port is only allowed to be zero in test mode.");
    }
  }

  public static void assertValidPort(final InetSocketAddress address, TachyonConf tachyonConf) {
    assertValidPort(address.getPort(), tachyonConf);
  }

  /**
   * Copy constructor to merge the properties of the incoming <code>TachyonConf</code>.
   *
   * @param tachyonConf The source {@link tachyon.conf.TachyonConf} to be merged.
   */
  public TachyonConf(TachyonConf tachyonConf) {
    merge(tachyonConf);
  }

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
   * 
   */
  public TachyonConf() {
    this(true);
  }

  /**
   * Test constructor for TachyonConfTest class.
   *
   * Most clients will call this constructor to allow default loading of properties to happen.
   *
   */
  TachyonConf(boolean includeSystemProperties) {
    loadDefault(includeSystemProperties);
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
    } else if (obj instanceof TachyonConf) {
      Properties props = ((TachyonConf) obj).getInternalProperties();
      return mProperties.equals(props);
    } else {
      return false;
    }
  }

  /**
   * Here is the order of importance of resource where we load the properties:
   *   -) System properties if desired
   *   -) Site specific properties via tachyon-site.properties file
   *   -) Default properties via tachyon-default.properties file
   * so we will load it in reverse order.
  */
  protected void loadDefault(boolean includeSystemProperties) {
    // Load default
    Properties defaultProps = new Properties();

    // Override runtime default
    defaultProps.setProperty(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName());
    defaultProps.setProperty(Constants.WORKER_NETWORK_NETTY_CHANNEL,
        ChannelType.defaultType().toString());
    defaultProps.setProperty(Constants.WORKER_MIN_WORKER_THREADS,
        String.valueOf(Runtime.getRuntime().availableProcessors()));
    defaultProps.setProperty(Constants.MASTER_MIN_WORKER_THREADS,
        String.valueOf(Runtime.getRuntime().availableProcessors()));

    InputStream defaultInputStream =
        TachyonConf.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES);
    if (defaultInputStream != null) {
      try {
        defaultProps.load(defaultInputStream);
      } catch (IOException e) {
        throw new RuntimeException("Unable to load default Tachyon properties file.", e);
      }
    } else {
      throw new RuntimeException("The default Tachyon properties file does not exist.");
    }

    // Update tachyon.master_address
    String masterHostname = defaultProps.getProperty(Constants.MASTER_HOSTNAME);
    String masterPort = defaultProps.getProperty(Constants.MASTER_PORT);
    boolean useZk = Boolean.parseBoolean(defaultProps.getProperty(Constants.USE_ZOOKEEPER));
    String masterAddress = (useZk ? Constants.HEADER_FT : Constants.HEADER) + masterHostname + ":"
        + masterPort;
    defaultProps.setProperty(Constants.MASTER_ADDRESS, masterAddress);

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
  }

  /**
   * @return the deep copy of the internal <code>Properties</code> of this TachyonConf instance.
   */
  public Properties getInternalProperties() {
    return SerializationUtils.clone(mProperties);
  }

  /**
   * Merge configuration properties with the other one. New one wins for duplicate
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

  public void set(String key, String value) {
    mProperties.put(key, value);
  }

  public String get(String key, final String defaultValue) {
    String raw = mProperties.getProperty(key, defaultValue);
    String updated = lookup(raw);
    LOG.debug("Get Tachyon property {} as {} with default {}", key, updated, defaultValue);
    return updated;
  }

  public int getInt(String key, final int defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Integer.parseInt(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as integer.");
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  public long getLong(String key, final long defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Long.parseLong(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as long.");
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  public double getDouble(String key, final double defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Double.parseDouble(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as double.");
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  public float getFloat(String key, final float defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return Float.parseFloat(lookup(rawValue));
      } catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate key " + key + " as float.");
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Boolean.parseBoolean(lookup(rawValue));
    } else {
      return defaultValue;
    }
  }

  public List<String> getList(String key, String delimiter, List<String> defaultValue) {
    if (delimiter == null) {
      throw new IllegalArgumentException("Illegal separator for Tachyon properties as list");
    }
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      return Lists.newLinkedList(Splitter.on(',').trimResults().omitEmptyStrings().split(rawValue));
    } else {
      return defaultValue;
    }
  }

  public <T extends Enum<T>> T getEnum(String key, T defaultValue) {
    if (mProperties.containsKey(key)) {
      final String val = get(key, defaultValue.toString());
      return null == val ? defaultValue : Enum.valueOf(defaultValue.getDeclaringClass(), val);
    } else {
      return defaultValue;
    }
  }

  public long getBytes(String key, long defaultValue) {
    String rawValue = get(key, "");
    try {
      return CommonUtils.parseSpaceSize(rawValue);
    } catch (Exception ex) {
      return defaultValue;
    }
  }

  public <T> Class<T> getClass(String key, Class<T> defaultValue) {
    if (mProperties.containsKey(key)) {
      String rawValue = mProperties.getProperty(key);
      try {
        return (Class<T>) Class.forName(rawValue);
      } catch (Exception e) {
        String msg = "requested class could not be loaded";
        LOG.error("{} : {} , {}", msg, rawValue, e);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code> implementing the
   * interface specified by <code>xface</code>.
   *
   * If no such property is specified, then <code>defaultValue</code> is returned.
   *
   * An exception is thrown if the returned class does not implement the named interface.
   *
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>, or <code>defaultValue</code>.
   */
  public <T> Class<? extends T> getClass(String name, Class<? extends T> defaultValue,
      Class<T> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else if (theClass != null) {
        return theClass.asSubclass(xface);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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
   * @return returns the key name with the ${key} substituted
   */
  String lookup(String base) {
    return lookup(base, new HashMap<String, String>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the String to look for.
   * @param found {@link Map} of String that already seen in this path.
   * @return resolved String value.
   */
  protected String lookup(final String base, Map<String, String> found) {
    // check argument
    if (base == null) {
      return null;
    }

    String resolved = base;
    // Lets find pattern match to ${key}. TODO: Consider using Apache Commons StrSubstitutor
    Matcher matcher = CONF_REGEX.matcher(base);
    while (matcher.find()) {
      String match = matcher.group(2).trim();
      String value;
      if (!found.containsKey(match)) {
        value = lookup(mProperties.getProperty(match), found);
        found.put(match, value);
      } else {
        value = found.get(match);
      }
      if (value != null) {
        LOG.debug("Replacing {} with {}", matcher.group(1), value);
        resolved = resolved.replaceFirst(REGEX_STRING, value);
      }
    }
    return resolved;
  }
}
