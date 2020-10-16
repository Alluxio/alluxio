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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.LongColumnStatsData;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbTable;
import alluxio.uri.Authority;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUdbTable implements UdbTable {
  private List<FieldSchema> mDataCols;
  private String mDbName;
  private String mName;
  private PartitionInfo mPartitionInfo;
  private Layout mTableLayout;
  private List<UdbPartition> mTestPartitions;
  private Schema mSchema;
  private List<FieldSchema> mPartitionCols;
  private List<ColumnStatisticsInfo> mStats;

  public TestUdbTable(String dbName, String name, int numOfPartitions, FileSystem fs) {
    mDbName = dbName;
    mName = name;
    mPartitionInfo = PartitionInfo.newBuilder()
        .setDbName(mDbName)
        .setTableName(mName)
        .setPartitionName(mName).build();
    mTableLayout = Layout.newBuilder()
        .setLayoutType(HiveLayout.TYPE)
        .setLayoutData(mPartitionInfo.toByteString())
        .build();
    FieldSchema col = FieldSchema.newBuilder().setName("col1")
        .setType("int").setId(1).build();
    FieldSchema col2 = FieldSchema.newBuilder().setName("col2")
        .setType("int").setId(2).build();
    mSchema = Schema.newBuilder().addCols(col).addCols(col2).build();
    mPartitionCols = Arrays.asList(col);
    mDataCols = Arrays.asList(col2);
    ColumnStatisticsInfo stats = ColumnStatisticsInfo.newBuilder().setColName("col2")
        .setColType("int").setData(ColumnStatisticsData.newBuilder()
            .setLongStats(LongColumnStatsData.getDefaultInstance()).build()).build();
    mStats = Arrays.asList(stats);

    mTestPartitions = Stream.iterate(0, n -> n + 1)
        .limit(numOfPartitions).map(i -> {
          AlluxioURI location = new AlluxioURI("/udbtable/"
              + CommonUtils.randomAlphaNumString(5) + i + "/test.csv");
          if (fs != null) {
            location = new AlluxioURI(Constants.SCHEME,
                Authority.fromString(String.join(",",
                    ConfigurationUtils.getMasterRpcAddresses(
                        fs.getConf()).stream()
                        .map(InetSocketAddress::toString)
                        .collect(ImmutableList.toImmutableList()))),
                "/udbtable/" + CommonUtils.randomAlphaNumString(5) + i + "/test.csv");
            try (FileOutStream out = fs.createFile(location,
                CreateFilePOptions.newBuilder().setRecursive(true).build())) {
              out.write("1".getBytes());
            } catch (IOException | AlluxioException e) {
              throw new RuntimeException(e);
            }

            final AlluxioURI waitLocation = location;
            try {
              CommonUtils.waitFor("file to be completed", () -> {
                try {
                  return fs.getStatus(waitLocation).isCompleted();
                } catch (Exception e) {
                  e.printStackTrace();
                  return false;
                }
              }, WaitForOptions.defaults().setTimeoutMs(100));
            } catch (InterruptedException | TimeoutException e) {
              throw new RuntimeException(e);
            }
          }
          return new TestPartition(new HiveLayout(genPartitionInfo(
              mDbName, mName, i, location.getParent().toString(), mDataCols), mStats));
        })
        .collect(Collectors.toList());
  }

  public static String getPartName(int index) {
    return "col1=" + index;
  }

  private static PartitionInfo genPartitionInfo(String dbName, String tableName, int index,
      String location, List<FieldSchema> dataCols) {
    return PartitionInfo.newBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setPartitionName(getPartName(index))
        .addAllDataCols(dataCols)
        .setStorage(Storage.newBuilder().setLocation(location).build())
        .build();
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
  public String getOwner() {
    return "testowner";
  }

  @Override
  public Map<String, String> getParameters() {
    return Collections.emptyMap();
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return mPartitionCols;
  }

  @Override
  public Layout getLayout() {
    return mTableLayout;
  }

  @Override
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStats;
  }

  @Override
  public List<UdbPartition> getPartitions() {
    return mTestPartitions;
  }

  private class TestPartition implements UdbPartition {
    private HiveLayout mLayout;

    private TestPartition(HiveLayout hiveLayout) {
      mLayout = hiveLayout;
    }

    @Override
    public String getSpec() {
      return mLayout.getSpec();
    }

    @Override
    public alluxio.table.common.Layout getLayout() {
      return mLayout;
    }
  }
}
