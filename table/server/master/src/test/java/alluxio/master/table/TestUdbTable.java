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

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUdbTable implements UdbTable {
  private String mDbName;
  private String mName;
  private PartitionInfo mPartitionInfo;

  public TestUdbTable(String dbName, String name) {
    mDbName = dbName;
    mName = name;
    mPartitionInfo = PartitionInfo.newBuilder()
        .setDbName(mDbName)
        .setTableName(mName)
        .setPartitionName(mName).build();
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Schema getSchema() {
    return Schema.getDefaultInstance();
  }

  @Override
  public String getOwner() {
    return "testowner";
  }

  @Override
  public Map<String, String> getParameters() {
    return Collections.emptyMap();
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return Collections.emptyList();
  }

  @Override
  public Layout getLayout() {
    return Layout.newBuilder()
        .setLayoutType(HiveLayout.TYPE)
        .setLayoutData(mPartitionInfo.toByteString())
        .build();
  }

  @Override
  public List<ColumnStatisticsInfo> getStatistics() {
    return Collections.emptyList();
  }

  @Override
  public List<UdbPartition> getPartitions() throws IOException {
    return Arrays.asList(new TestPartition(
        new HiveLayout(mPartitionInfo, Collections.emptyList())));
  }

  private class TestPartition implements UdbPartition{
    private alluxio.table.common.Layout mLayout;

    private TestPartition(HiveLayout hiveLayout) {
      mLayout = hiveLayout;
    }

    @Override
    public String getSpec() {
      return "testPartition";
    }

    @Override
    public alluxio.table.common.Layout getLayout() {
      return mLayout;
    }
  }
}
