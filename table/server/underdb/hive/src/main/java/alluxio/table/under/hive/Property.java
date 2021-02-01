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

package alluxio.table.under.hive;

import alluxio.table.common.udb.UdbProperty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This contains all the properties for this UDB.
 */
public final class Property extends UdbProperty {
  private static final Logger LOG = LoggerFactory.getLogger(Property.class);
  /** A map from default property key's string name to the key. */
  private static final Map<String, Property> DEFAULT_KEYS_MAP = new ConcurrentHashMap<>();

  /**
   * Create a alluxio.table.under.hive.Property instance.
   *
   * @param name the property name
   * @param description the property description
   * @param defaultValue the default value
   */
  public Property(String name, String description, String defaultValue) {
    super(name, description, defaultValue);
  }

  /**
   * UDB property builder.
   */
  public static final class Builder {
    private String mName;
    private String mDescription;
    private String mDefaultValue;

    /**
     * @param name name of property
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param name name for the property
     * @return the updated builder instance
     */
    public Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param defaultValue the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultValue(String defaultValue) {
      mDefaultValue = defaultValue;
      return this;
    }

    /**
     * @param description of the property
     * @return the updated builder instance
     */
    public Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * Register the unregistered udb property.
     *
     * @return registered udb property
     */
    public Property build() {
      Property property = buildUnregistered();
      Preconditions.checkState(
          Property.register(property),
          "Cannot register existing alluxio.table.under.hive.Property \"%s\"", mName);
      return property;
    }

    /**
     * Creates the Udb alluxio.table.under.hive.Property
     * without registering it with default property list.
     *
     * @return udb property
     */
    public Property buildUnregistered() {
      Property property = new Property(mName, mDescription, mDefaultValue);
      return property;
    }
  }

  /**
   * Registers the given UDB alluxio.table.under.hive.Property to the global map.
   *
   * @param Property the udb property
   * @return whether the udb property is successfully registered
   */
  @VisibleForTesting
  public static boolean register(Property Property) {
    String name = Property.getName();
    if (DEFAULT_KEYS_MAP.containsKey(name)) {
      return false;
    }

    DEFAULT_KEYS_MAP.put(name, Property);
    return true;
  }

  /**
   * Unregisters the given key from the global map.
   *
   * @param Property the property to unregister
   */
  @VisibleForTesting
  public static void unregister(Property Property) {
    String name = Property.getName();
    DEFAULT_KEYS_MAP.remove(name);
  }

  public static final Property ALLOW_DIFF_PART_LOC_PREFIX =
      new Builder(Name.ALLOW_DIFF_PART_LOC_PREFIX)
          .setDefaultValue("false")
          .setDescription("Whether to mount partitions diff location prefix partitions")
          .build();

  /**
   * @return the name of alluxio.table.under.hive.Property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the description of a property
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the default value of udb property or null if value not set
   */
  @Nullable
  @Override
  public String getDefaultValue() {
    Object defaultValue = mDefaultValue;
    return defaultValue == null ? null : defaultValue.toString();
  }

  /**
   * Corresponding configurations of HIVE configurations.
   */
  public static final class Name {
    // Hive related properties
    public static final String ALLOW_DIFF_PART_LOC_PREFIX = "allow.diff.partition.location.prefix";
  }
}
