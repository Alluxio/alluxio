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

package alluxio.table.common.udb;

import alluxio.table.common.BaseProperty;
import alluxio.table.common.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a property name and default value for the UDB.
 */
public class UdbProperty extends BaseProperty {
  private static final Logger LOG = LoggerFactory.getLogger(UdbProperty.class);

  /**
   * Creates an instance.
   *
   * @param name the property name
   * @param description the property description
   * @param defaultValue the default value
   */
  public UdbProperty(String name, String description, String defaultValue) {
    super(name, description, defaultValue);
  }

  /**
   * @param udbType the udb type
   * @return returns the full name of the property, including the prefix
   */
  public String getFullName(String udbType) {
    return String.format("%s%s", ConfigurationUtils.getUdbPrefix(udbType), mName);
  }
}
