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

import static java.util.stream.Collectors.toSet;

import alluxio.PropertyKey;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides the source of truth of property values and a unified abstraction to put and get
 * properties, hiding the difference of accessing user-specified properties, the default properties
 * (known at construction time) and the extension properties (known at runtime). This class is
 * supposed to handle the ordering and priority of properties from different sources, whereas the
 * <code>Configuration</code> class is supposed to handle the type conversion on top of the source
 * of truth of the properties.
 *
 * For a given property key, the order of preference of its value is (from highest to lowest)
 *
 * (1) hadoop config
 * (2) system properties,
 * (3) properties in the specified file (site-properties),
 * (4) default property values.
 */
@NotThreadSafe
public class AlluxioProperties {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioProperties.class);

  /**
   * Map of user-specified properties. When key is mapped to Optional.empty(), it indicates no
   * value is set for this key. Note that, ConcurrentHashMap requires not null for key and value.
   */
  private final ConcurrentHashMap<String, Optional<String>> mUserProps = new ConcurrentHashMap<>();
  /** Map of property sources. */
  private final ConcurrentHashMap<String, Source> mSources = new ConcurrentHashMap<>();

  /**
   * Constructs a new instance of Alluxio properties.
   */
  public AlluxioProperties() {}

  /**
   * Factory method to initialize the properties based on given source and properties pairs.
   *
   * @param sourceMap the map from source to properties
   * @return the created instance of Alluxio properties
   */
  public static AlluxioProperties create(Map<Source, Map<?, ?>> sourceMap) {
    AlluxioProperties properties = new AlluxioProperties();
    // Keep the order of sources from lowest to highest
    for (Source source : Arrays.asList(Source.SITE_PROPERTY, Source.SYSTEM_PROPERTY,
        Source.HADOOP_CONF)) {
      if (sourceMap.containsKey(source)) {
        Map<?, ?> map = sourceMap.get(source);
        properties.merge(map, source);
      }
    }
    return properties;
  }

  /**
   * @param key the key to query
   * @return the value, or null if the key has no value set
   */
  @Nullable
  public String get(String key) {
    if (mUserProps.containsKey(key)) {
      return mUserProps.get(key).orElse(null);
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
    mUserProps.put(key, Optional.ofNullable(value));
  }

  /**
   * Remove the value set for key.
   *
   * @param key key to remove
   */
  public void remove(String key) {
    mUserProps.put(key, Optional.empty());
  }

  /**
   * Checks if there is a value set for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public boolean hasValueSet(String key) {
    if (mUserProps.containsKey(key)) {
      return mUserProps.get(key).isPresent();
    }
    return PropertyKey.isValid(key) && (PropertyKey.fromString(key).getDefaultValue() != null);
  }

  /**
   * @return the entry set of all Alluxio property key and value pairs (value can be null)
   */
  public Set<Map.Entry<String, String>> entrySet() {
    return keySet().stream().map(key -> Maps.immutableEntry(key, get(key))).collect(toSet());
  }

  /**
   * @return the key set of all Alluxio property
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
   * @param properties the source {@link Properties} to be merged
   * @param source the source of the the properties (e.g., system property, default and etc)
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
        // This will register the key as a valid PropertyKey
        // TODO(adit): Do not add properties unrecognized by Ufs extensions when Configuration
        // is made dynamic
        put(new PropertyKey.Builder(key).build().getName(), value);
        setSource(key, source);
      }
    }
  }
}
