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

package alluxio.client.hive;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Data structure that stores and returns Hive related Cache Scope
 */
public class HiveCacheContext
{
  private final String mDatabase;
  private final String mTable;
  private final String mPartition;

  public HiveCacheContext(String database, String table, String partition)
  {
    this.mDatabase = database;
    this.mTable = table;
    this.mPartition = partition;
  }

  public String getDatabase()
  {
    return mDatabase;
  }

  public String getTable()
  {
    return mTable;
  }

  public String getPartition()
  {
    return mPartition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HiveCacheContext that = (HiveCacheContext) o;
    return java.util.Objects.equals(mDatabase, that.mDatabase)
        && java.util.Objects.equals(mTable, that.mTable)
        && java.util.Objects.equals(mPartition, that.mPartition);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(mDatabase, mTable, mPartition);
  }

  @Override
  public String toString()
  {
    return MoreObjects.toStringHelper(this)
        .add("database", mDatabase)
        .add("table", mTable)
        .add("Partition", mPartition)
        .toString();
  }
}
