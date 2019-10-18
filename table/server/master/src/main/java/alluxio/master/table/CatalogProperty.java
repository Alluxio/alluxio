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

package alluxio.master.table;

import alluxio.table.common.BaseProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a property name and default value for the catalog.
 */
public class CatalogProperty extends BaseProperty {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogProperty.class);

  private CatalogProperty(String name, String description, String defaultValue) {
    super(name, description, defaultValue);
  }

  public static final CatalogProperty DB_TYPE =
      new CatalogProperty("db.type", "The type of database. Possible values are: hive", "");
}
