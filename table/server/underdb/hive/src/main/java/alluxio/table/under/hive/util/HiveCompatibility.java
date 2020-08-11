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

package alluxio.table.under.hive.util;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * Hive compatibility.
 */
public interface HiveCompatibility {
  /**
   * Get table operation.
   * @param dbname database name
   * @param name table name
   * @return Table object
   */
  Table getTable(String dbname, String name)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Test if a table exists.
   * @param dbname database name
   * @param name table name
   * @return true if the table exists
   */
  boolean tableExists(String dbname, String name)
      throws MetaException, TException, NoSuchObjectException;
}
