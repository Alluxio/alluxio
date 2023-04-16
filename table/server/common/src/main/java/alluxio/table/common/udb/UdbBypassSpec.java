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

import java.util.Map;
import java.util.Set;

/**
 * Tables and partitions bypassing specification.
 */
public final class UdbBypassSpec {
  /**
   * Map of table name to set of partition names.
   * Keyed by a table's name, the value set contains names of partitions in that table.
   * An empty set indicates all partitions of that table, if any, should be bypassed.
   */
  private final Map<String, Set<String>> mTablePartMap;

  /**
   * @param tablePartMap table to partition map
   */
  public UdbBypassSpec(Map<String, Set<String>> tablePartMap) {
    mTablePartMap = tablePartMap;
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbBypassSpec#hasFullTable(String)
   */
  public boolean hasTable(String tableName) {
    return mTablePartMap.containsKey(tableName);
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbBypassSpec#hasTable(String)
   */
  public boolean hasFullTable(String tableName) {
    // empty set indicates all partitions should be bypassed
    return hasTable(tableName) && mTablePartMap.get(tableName).size() == 0;
  }

  /**
   * Checks by a partition's name if it should be bypassed.
   *
   * @param tableName the table name
   * @param partitionName the partition name
   * @return true if the partition should be bypassed, false otherwise
   */
  public boolean hasPartition(String tableName, String partitionName) {
    if (!hasTable(tableName)) {
      return false;
    }
    Set<String> parts = mTablePartMap.get(tableName);
    if (parts.size() == 0) {
      // empty set indicates all partitions should be bypassed
      return true;
    }
    return parts.contains(partitionName);
  }
}
