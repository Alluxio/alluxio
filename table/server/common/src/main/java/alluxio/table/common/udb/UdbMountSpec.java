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
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbMountSpec {
  /**
   * Map of table name to set of partition names.
   * Keyed by a table's name, the value set contains names of partitions in that table.
   * An empty set indicates all partitions of that table, if any, should be bypassed.
   */
  private final Map<String, Set<String>> mBypassed;

  /**
   * Set of ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   */
  private final Set<String> mIgnored;

  /**
   * @param bypassed bypassed table to partition map
   * @param ignored ignored tables set
   */
  public UdbMountSpec(Map<String, Set<String>> bypassed, Set<String> ignored) {
    mBypassed = bypassed;
    mIgnored = ignored;
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbMountSpec#hasFullyBypassedTable(String)
   */
  public boolean hasBypassedTable(String tableName) {
    return mBypassed.containsKey(tableName);
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbMountSpec#hasBypassedTable(String)
   */
  public boolean hasFullyBypassedTable(String tableName) {
    // empty set indicates all partitions should be bypassed
    return hasBypassedTable(tableName) && mBypassed.get(tableName).size() == 0;
  }

  /**
   * Checks if a table is ignored.
   *
   * @param tableName the table name
   * @return true if the table is ignored, false otherwise
   */
  public boolean hasIgnoredTable(String tableName) {
    return mIgnored.contains(tableName);
  }

  /**
   * @return tables ignored
   */
  public Set<String> getIgnoredTables() {
    return mIgnored;
  }

  /**
   * Checks by a partition's name if it should be bypassed.
   *
   * @param tableName the table name
   * @param partitionName the partition name
   * @return true if the partition should be bypassed, false otherwise
   */
  public boolean hasBypassedPartition(String tableName, String partitionName) {
    if (!hasBypassedTable(tableName)) {
      return false;
    }
    Set<String> parts = mBypassed.get(tableName);
    if (parts.size() == 0) {
      // empty set indicates all partitions should be bypassed
      return true;
    }
    return parts.contains(partitionName);
  }
}
