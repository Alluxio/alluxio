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

package alluxio.table.under.gdc;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.udb.UdbTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * GDC table implementation.
 * */
public class GDCTable implements UdbTable {
  private static final Logger LOG = LoggerFactory.getLogger(GDCTable.class);

  private final String mName;
  private final Schema mSchema;
  private final String mOwner;
  private final List<ColumnStatisticsInfo> mStatistics;
  private final List<FieldSchema> mPartitionKeys;
  private final List<UdbPartition> mUdbPartitions;
  private final Map<String, String> mParameters;
  private final Layout mLayout;

  /**
   * Create a new glue table instance.
   *
   * @param name the table name
   * @param schema the table schema
   * @param cols list of partition keys
   * @param udbPartitions list of partitions
   * @param statistics the table statistics
   * @param layout the table layout
   */
  public GDCTable(String name, Schema schema, List<ColumnStatisticsInfo> statistics,
                   List<FieldSchema> cols, List<UdbPartition> udbPartitions, Layout layout) {
    mUdbPartitions = udbPartitions;
    mName = name;
    mSchema = schema;
    mStatistics = statistics;
    mPartitionKeys = cols;
    mOwner = "null";
    mParameters = Collections.emptyMap();
    mLayout = layout;
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
    return mOwner;
  }

  @Override
  public Map<String, String> getParameters() {
    return mParameters;
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return mPartitionKeys;
  }

  @Override
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStatistics;
  }

  @Override
  public Layout getLayout() {
    return mLayout;
  }

  @Override
  public List<UdbPartition> getPartitions() {
    return mUdbPartitions;
  }
}
