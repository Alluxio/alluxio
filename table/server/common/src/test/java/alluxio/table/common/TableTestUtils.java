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

package alluxio.table.common;

import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.table.common.layout.HiveLayout;

import java.util.Collections;

public class TableTestUtils {
  /**
   * @param location the layout's location
   * @return a layout for the location
   */
  public static HiveLayout createLayout(String location) {
    PartitionInfo partitionInfo = PartitionInfo.newBuilder()
        .setStorage(Storage.newBuilder().setLocation(location).build())
        .build();
    return new HiveLayout(partitionInfo, Collections.emptyList());
  }

  private TableTestUtils() {} // Prevent initialization
}
