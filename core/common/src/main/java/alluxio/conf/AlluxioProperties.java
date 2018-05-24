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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
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
 * (1) runtime config
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
  private final ConcurrentHashMap<PropertyKey, Optional<String>> mUserProps =
      new ConcurrentHashMap<>();
  /** Map of property sources. */
  private final ConcurrentHashMap<PropertyKey, Source> mSources = new ConcurrentHashMap<>();

  /**
   * Constructs a new instance of Alluxio properties.
   */
  public AlluxioProperties() {}

  /**
   * @param key the key to query
   * @return the value, or null if the key has no value set
   */
  @Nullable
  public String get(PropertyKey key) {
    if (mUserProps.containsKey(key)) {
      return mUserProps.get(key).orElse(null);
    }
    // In case key is not the reference to the original key
    return PropertyKey.fromString(key.toString()).getDefaultValue();
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
   * @param source the source of this value for the key
   */
  public void put(PropertyKey key, String value, Source source) {
    if (!mUserProps.containsKey(key) || source.compareTo(getSource(key)) >= 0) {
      mUserProps.put(key, Optional.ofNullable(value));
      mSources.put(key, source);
    }
  }

  /**
   * Merges the current configuration properties with new properties. If a property exists
   * both in the new and current configuration, the one from the new configuration wins if
   * its priority is higher or equal than the existing one.
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
      PropertyKey propertyKey;
      if (PropertyKey.isValid(key)) {
        propertyKey = PropertyKey.fromString(key);
      } else {
        // Add unrecognized properties
        LOG.debug("Property {} from source {} is unrecognized", key, source);
        // Workaround for issue https://alluxio.atlassian.net/browse/ALLUXIO-3108
        // This will register the key as a valid PropertyKey
        // TODO(adit): Do not add properties unrecognized by Ufs extensions when Configuration
        // is made dynamic
        propertyKey = new PropertyKey.Builder(key).build();
      }
      put(propertyKey, value, source);
    }
  }

  /**
   * Remove the value set for key.
   *
   * @param key key to remove
   */
  public void remove(PropertyKey key) {
    mUserProps.put(key, Optional.empty());
  }

  /**
   * Checks if there is a value set for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  public boolean hasValueSet(PropertyKey key) {
    if (mUserProps.containsKey(key)) {
      return mUserProps.get(key).isPresent();
    }
    // In case key is not the reference to the original key
    return PropertyKey.fromString(key.toString()).getDefaultValue() != null;
  }

  /**
   * @return the entry set of all Alluxio property key and value pairs (value can be null)
   */
  public Set<Map.Entry<PropertyKey, String>> entrySet() {
    return keySet().stream().map(key -> Maps.immutableEntry(key, get(key))).collect(toSet());
  }

  /**
   * @return the key set of all Alluxio property
   */
  public Set<PropertyKey> keySet() {
    Set<PropertyKey> keySet = new HashSet<>(PropertyKey.defaultKeys());
    keySet.addAll(mUserProps.keySet());
    return keySet;
  }

  /**
   * Iterates over all the key value pairs and performs the given action.
   *
   * @param action the operation to perform on each key value pair
   */
  public void forEach(BiConsumer<? super PropertyKey, ? super String> action) {
    for (Map.Entry<PropertyKey, String> entry : entrySet()) {
      action.accept(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Sets the source for a given key.
   *
   * @param key property key
   * @param source the source
   */
  @VisibleForTesting
  public void setSource(PropertyKey key, Source source) {
    mSources.put(key, source);
  }

  /**
   * @param key property key
   * @return the source of the key
   */
  public Source getSource(PropertyKey key) {
    Source source = mSources.get(key);
    if (source != null) {
      return source;
    }
    return Source.DEFAULT;
  }
}
