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

package alluxio.table.common;

import alluxio.exception.ExceptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This represents a configuration of the catalog.
 *
 * @param <T> the type of property that this instance is used for
 */
public abstract class BaseConfiguration<T extends BaseProperty> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseConfiguration.class);

  protected final ConcurrentHashMap<String, String> mValues;

  protected BaseConfiguration() {
    mValues = new ConcurrentHashMap<>();
  }

  /**
   * Creates an instance.
   *
   * @param values the map of values to copy from
   */
  public BaseConfiguration(Map<String, String> values) {
    mValues = new ConcurrentHashMap<>();
    mValues.putAll(values);
  }

  /**
   * Returns the value of this property, or the default value if the property is not defined.
   *
   * @param property the property to get the value for
   * @return the property value
   */
  public String get(T property) {
    String value = mValues.get(property.getName());
    if (value == null) {
      return property.getDefaultValue();
    }
    return value;
  }

  /**
   * Return the int value of this property , or the default value if the property is not defined.
   *
   * @param property the property to get the int value
   * @return the int value of property
   */
  public int getInt(T property) {
    String rawValue = get(property);

    try {
      return Integer.parseInt(rawValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          ExceptionMessage.KEY_NOT_INTEGER.getMessage(rawValue, property));
    }
  }

  /**
   * Return the boolean value of this property.
   *
   * @param property the property to get the boolean value
   * @return the boolean value of property
   */
  public boolean getBoolean(T property) {
    String rawValue = get(property);

    try {
      return Boolean.parseBoolean(rawValue);
    } catch (Exception e) {
      throw new RuntimeException(
          ExceptionMessage.KEY_NOT_BOOLEAN.getMessage(rawValue, property));
    }
  }

  /**
   * @return the full map of the configuration
   */
  public Map<String, String> getMap() {
    return mValues;
  }
}
