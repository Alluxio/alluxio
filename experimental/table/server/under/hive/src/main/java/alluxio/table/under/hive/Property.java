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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This contains all the properties for this UDB.
 */
public final class Property {
  private static final Logger LOG = LoggerFactory.getLogger(Property.class);

  public static final UdbProperty DATABASE_NAME =
      new UdbProperty("database-name", "The name of the database in the hive metastore.", "");
  public static final UdbProperty HIVE_METASTORE_URIS =
      new UdbProperty("hive.metastore.uris", "The thrift URI for the hive metastore.", "");
}
