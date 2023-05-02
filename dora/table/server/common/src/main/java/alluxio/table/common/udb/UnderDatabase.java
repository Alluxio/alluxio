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

import alluxio.master.table.DatabaseInfo;

import java.io.IOException;
import java.util.List;

/**
 * The database interface.
 */
public interface UnderDatabase {

  /**
   * @return the database type
   */
  String getType();

  /**
   * @return the database name
   */
  String getName();

  /**
   * @return a list of table names
   */
  List<String> getTableNames() throws IOException;

  /**
   * @param tableName the table name
   * @param bypassSpec table and partition bypass specification
   * @return the {@link UdbTable} for the specified table name
   */
  UdbTable getTable(String tableName, UdbBypassSpec bypassSpec) throws IOException;

  /**
   * @return the {@link UdbContext}
   */
  UdbContext getUdbContext();

  /**
   * @return get database info
   */
  DatabaseInfo getDatabaseInfo() throws IOException;
}
