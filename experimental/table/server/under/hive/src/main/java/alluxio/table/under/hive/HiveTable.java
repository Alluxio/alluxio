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

import alluxio.grpc.FileStatistics;
import alluxio.grpc.Schema;
import alluxio.table.common.UdbTable;

import java.util.Map;

/**
 * Hive table implementation.
 */
public class HiveTable implements UdbTable {
  private final String mName;
  private final Schema mSchema;
  private final String mBaseLocation;
  private final Map<String, FileStatistics> mStatistics;

  /**
   * Creates a new instance.
   *
   * @param name the table name
   * @param schema the table schema
   * @param baseLocation the base location
   * @param statistics the table statistics
   */
  public HiveTable(String name, Schema schema, String baseLocation,
      Map<String, FileStatistics> statistics) {
    mName = name;
    mSchema = schema;
    mBaseLocation = baseLocation;
    mStatistics = statistics;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public String getBaseLocation() {
    return mBaseLocation;
  }

  @Override
  public Map<String, FileStatistics> getStatistics() {
    return mStatistics;
  }
}
