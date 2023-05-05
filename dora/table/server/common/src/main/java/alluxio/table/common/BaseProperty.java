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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a property name and default value for the UDB.
 */
public abstract class BaseProperty {
  private static final Logger LOG = LoggerFactory.getLogger(BaseProperty.class);

  protected final String mName;
  protected final String mDescription;
  protected final String mDefaultValue;

  protected BaseProperty(String name, String description, String defaultValue) {
    mName = name;
    mDescription = description;
    mDefaultValue = defaultValue;
  }

  /**
   * @return the property name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the property description
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the property default value
   */
  public String getDefaultValue() {
    return mDefaultValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseProperty that = (BaseProperty) o;
    return mName.equals(that.mName);
  }

  @Override
  public int hashCode() {
    return mName.hashCode();
  }
}
