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

import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.HiveTableInfo;
import alluxio.grpc.catalog.Schema;
import alluxio.grpc.catalog.UdbTableInfo;
import alluxio.table.common.UdbPartition;

import java.io.IOException;
import java.util.List;

/**
 * The interface for the underdb table.
 */
public interface UdbTable {

  /**
   * @return the table name
   */
  String getName();

  /**
   * @return the table schema
   */
  Schema getSchema();

  // TODO(gpang): generalize statistics
  /**
   * @return statistics of the table
   */
  List<ColumnStatisticsInfo> getStatistics();

  /**
   * @return returns partitions for the table
   */
  List<UdbPartition> getPartitions() throws IOException;

  /**
   * @return returns a proto representing the table
   */
  UdbTableInfo toProto() throws IOException;

  /**
   * @param proto UdbTableInfo in proto form
   * @return UdbTable object
   */
  static UdbTable fromProto(UdbTableInfo proto) {
    if (proto.hasHiveTableInfo()) {
      HiveTableInfo tableInfo = proto.getHiveTableInfo();

    }
  }

  boolean isConnected();
}
