/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.conf;

import static java.util.stream.Collectors.toSet;

import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides the source of truth of property values and a unified abstraction to put and get
 * properties, hiding the difference of accessing user-specified properties, the default properties
 * (known at construction time) and the extension properties (known at runtime).
 * This class is supposed to handle the ordering and priority of properties from different sources,
 * whereas the <code>Configuration</code> class is supposed to handle the type conversion on top of
 * the source of truth of the properties.
 *
 * For a given property key, the order of preference of its value is (from highest to lowest)
 *
 * (1) system properties,
 * (2) properties in the specified file (site-properties),
 * (3) default property values.
 */
@NotThreadSafe
public class AlluxioProperties {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioProperties.class);

  /** Map of user-specified properties. Value CANNOT be null. */
  private final ConcurrentHashMap<String, String> mUserProps = new ConcurrentHashMap<>();
  /** Map of property sources. */
  private final ConcurrentHashMap<String, Source> mSources = new ConcurrentHashMap<>();

  private final Map<Source, Properties> mPropBySource = new HashMap<>();
  private final String mSitePropertyFile;

  /**
   * Constructs a new instance of Alluxio properties.
   */
  private AlluxioProperties(String sitePropertyFile) {
    mSitePropertyFile = sitePropertyFile;
  }

  /**
   * Factory method to initialize the default properties.
   *
   */
  public static AlluxioProperties create() {
    // Load system properties
    Properties systemProps = new Properties();
    systemProps.putAll(System.getProperties());

    String sitePropertyFile = "";
    Properties siteProps = new Properties();
    // Load site specific properties file if not in test mode. Note that we decide whether in test
    // mode by default properties and system properties (via getBoolean). If it is not in test mode
    // the PROPERTIES will be updated again.
    if (!Boolean.valueOf(PropertyKey.TEST_MODE.getDefaultValue())
        && !Boolean.valueOf(systemProps.getProperty(PropertyKey.TEST_MODE.getName()))) {
      // we are not in test mode, load site properties
      String confPaths = PropertyKey.SITE_CONF_DIR.getDefaultValue();
      if (systemProps.contains(PropertyKey.SITE_CONF_DIR.getName())) {
        confPaths = systemProps.getProperty(PropertyKey.SITE_CONF_DIR.getName());
      }
      String[] confPathList = confPaths.split(",");
      sitePropertyFile =
          ConfigurationUtils.searchPropertiesFile(Constants.SITE_PROPERTIES, confPathList);
      if (sitePropertyFile != null) {
        siteProps = ConfigurationUtils.loadPropertiesFromFile(sitePropertyFile);
        LOG.info("Configuration file {} loaded.", sitePropertyFile);
      } else {
        siteProps = ConfigurationUtils.loadPropertiesFromResource(Constants.SITE_PROPERTIES);
      }
    }
    // Now lets combine, order matters here
    AlluxioProperties properties = new AlluxioProperties(sitePropertyFile);
    properties.merge(siteProps, Source.SITE_PROPERTY);
    properties.merge(systemProps, Source.SYSTEM_PROPERTY);
    return properties;
  }

  /**
   * @param key the key to query
   * @return the value, or null if the key has no value set
   */
  @Nullable
  public String get(String key) {
    if (mUserProps.containsKey(key)) {
      return mUserProps.get(key);
    }
    // fromString will check if the key is valid or throw RTE
    return PropertyKey.fromString(key).getDefaultValue();
  }

  /**
   * Clears all existing user-specified properties.
   */
  public void clear() {
    mUserProps.clear();
    mSources.clear();
  }

  /**
   * Puts the key value pair specified by users.
   *
   * @param key key to put
   * @param value value to put
   */
  public void put(String key, String value) {
    mUserProps.put(key, value);
  }

  /**
   * Remove the key and value.
   *
   * @param key key to put
   */
  public void remove(String key) {
    mUserProps.remove(key);
  }

  /**
   * Checks if there is a value set for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public boolean hasValueSet(String key) {
    if (mUserProps.contains(key)) {
      return true;
    }
    return PropertyKey.isValid(key) && (PropertyKey.fromString(key).getDefaultValue() != null);
  }

  /**
   * @return the entry set of all Alluxio property key and value pairs (value can be null)
   */
  public Set<Map.Entry<String, String>> entrySet() {
    Set<Map.Entry<String, String>> entrySet = PropertyKey.defaultKeys().stream()
        .filter(key -> !mUserProps.contains(key.getName()))
        .map(key -> Maps.immutableEntry(key.getName(), key.getDefaultValue()))
        .collect(toSet());
    entrySet.addAll(mUserProps.entrySet());
    return entrySet;
  }

  /**
   * @return the entry set of all Alluxio property key and value pairs (value can be null)
   */
  public Set<String> keySet() {
    Set<String> keySet =
        PropertyKey.defaultKeys().stream().map(PropertyKey::getName).collect(toSet());
    keySet.addAll(mUserProps.keySet());
    return keySet;
  }

  /**
   * Iterates over all the key value pairs and performs the given action.
   *
   * @param action the operation to perform on each key value pair
   */
  public void forEach(BiConsumer<? super String, ? super String> action) {
    for (Map.Entry<String, String> entry : entrySet()) {
      action.accept(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Sets the source for a given key.
   *
   * @param key property key
   * @param source the source
   */
  public void setSource(String key, Source source) {
    mSources.put(key, source);
  }

  /**
   * @param key property key
   * @return the source of the key
   */
  public Source getSource(String key) {
    Source source = mSources.get(key);
    if (source != null) {
      return source;
    }
    if (PropertyKey.isValid(key)) {
      return Source.DEFAULT;
    }
    return Source.UNKNOWN;
  }

  /**
   * Merges the current configuration properties with alternate properties. A property from the new
   * configuration wins if it also appears in the current configuration.
   *
   * @param properties The source {@link Properties} to be merged
   * @param source The source of the the properties (e.g., system property, default and etc)
   */
  public void merge(Map<?, ?> properties, Source source) {
    if (properties == null) {
      return;
    }
    // merge the properties
    for (Map.Entry<?, ?> entry : properties.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue() == null ? null : entry.getValue().toString().trim();
      if (PropertyKey.isValid(key)) {
        PropertyKey propertyKey = PropertyKey.fromString(key);
        // Get the true name for the property key in case it is an alias.
        put(propertyKey.getName(), value);
        setSource(propertyKey.getName(), source);
      } else {
        // Add unrecognized properties
        LOG.debug("Property {} from source {} is unrecognized", key, source);
        // Workaround for issue https://alluxio.atlassian.net/browse/ALLUXIO-3108
        // TODO(adit): Do not add properties unrecognized by Ufs extensions when Configuration
        // is made dynamic
        put(key, value);
        setSource(key, source);
      }
    }
  }

  /**
   * @return the path to the site property file
   */
  public String getSitePropertiesFile() {
    return mSitePropertyFile;
  }
}
