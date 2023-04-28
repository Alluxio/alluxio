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

  public static final int DEFAULT_DB_SYNC_THREADS = 4;

  private CatalogProperty(String name, String description, String defaultValue) {
    super(name, description, defaultValue);
  }

  public static final CatalogProperty DB_IGNORE_TABLES =
      new CatalogProperty("catalog.db.ignore.udb.tables",
          "The comma-separated list of table names to ignore from the UDB.", "");
  public static final CatalogProperty DB_SYNC_THREADS =
      new CatalogProperty("catalog.db.sync.threads",
          "The maximum number of threads to use when parallel syncing all the tables from the "
              + "under database (UDB) to the catalog. If this is set too large, the threads may "
              + "overload the UDB, and if set too low, syncing a database with many tables may "
              + "take a long time.",
          Integer.toString(DEFAULT_DB_SYNC_THREADS));
  public static final CatalogProperty DB_CONFIG_FILE =
      new CatalogProperty("catalog.db.config.file",
          "The config file for the UDB.", "<catalog.db.config.file>");
}
