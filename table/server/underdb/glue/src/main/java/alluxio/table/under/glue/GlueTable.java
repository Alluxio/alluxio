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

package alluxio.table.under.glue;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.GlueLayout;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.under.glue.util.PathTranslator;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Glue table implementation.
 */
public class GlueTable implements UdbTable {
  private static final Logger LOG = LoggerFactory.getLogger(GlueTable.class);

  private final GlueDatabase mGlueDatabase;
  private final PathTranslator mPathTranslator;
  private final String mName;
  private final String mOwner;
  private final Table mTable;
  private final List<FieldSchema> mPartitionKeys;
  private final Map<String, String> mParameters;
  private final List mPartitions;
  private final List<ColumnStatisticsInfo> mStatistics;
  private final Schema mSchema;
  private final Layout mLayout;

  /**
   * Create a new glue table instance.
   *
   * @param glueDatabase the glue udb
   * @param pathTranslator the glue to alluxio path translator
   * @param name the table name
   * @param schema the table schema
   * @param cols list of partition keys
   * @param partitions list of partitions
   * @param statistics the table statistics
   * @param layout the table layout
   * @param table glue table object
   */
  public GlueTable(GlueDatabase glueDatabase, PathTranslator pathTranslator, String name,
      Schema schema, List<ColumnStatisticsInfo> statistics, List<FieldSchema> cols,
      List<Partition> partitions, Layout layout, Table table) {
    mGlueDatabase = glueDatabase;
    mPathTranslator = pathTranslator;
    mTable = table;
    mName = name;
    mSchema = schema;
    mPartitions = partitions;
    mPartitionKeys = cols;
    mStatistics = statistics;
    mOwner = (table.getOwner() != null) ? table.getOwner() : null;
    mParameters = (table.getParameters() != null) ? table.getParameters() : Collections.emptyMap();
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
    // TODO(shouwei): refact code to align to latest version
    List<UdbPartition> udbPartitions = new ArrayList<>();
    try {
      List<Partition> partitions = mPartitions;
      List<String> dataColumns = mTable.getStorageDescriptor().getColumns().stream()
          .map(Column::getName)
          .collect(Collectors.toList());
      List<String> partitionColumns = mTable.getPartitionKeys().stream()
          .map(Column::getName)
          .collect(Collectors.toList());

      if (partitionColumns.isEmpty()) {
        PartitionInfo.Builder partitionInfoBuilder = PartitionInfo.newBuilder()
            .setDbName(mGlueDatabase.getUdbContext().getDbName()).setTableName(mName)
            .addAllDataCols(GlueUtils.toProto(mTable.getStorageDescriptor().getColumns()))
            .setStorage(GlueUtils.toProto(mTable.getStorageDescriptor(), mPathTranslator))
            .setPartitionName(mName)
            .putAllParameters(mTable.getParameters());
        udbPartitions.add(new GluePartition(
            new GlueLayout(partitionInfoBuilder.build(), Collections.emptyList())));
        return udbPartitions;
      }
      // Glue partition name place holder
      List<String> partitionNames = partitions.stream()
          .map(partition -> partition.getValues().toString()).collect(Collectors.toList());
    } catch (EntityNotFoundException e) {
      LOG.warn("Table " + mTable.getName() + " does not exist.", e);
    } catch (IOException e) {
      LOG.warn("Table " + mTable.getName() + " does not exist.", e);
    }
    return udbPartitions;
  }
}
