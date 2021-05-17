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

import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Google Data Catalog database implementation.
 */
public class GDCDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(GDCDatabase.class);

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;
  private final String mGdcDatasetName;
  private final String mGdcProjectId;

  /* placeholder values */
  private final String mOwnerName = "";
  private final alluxio.grpc.table.PrincipalType mOwnerType = alluxio.grpc.table.PrincipalType.ROLE;

  @VisibleForTesting
  protected GDCDatabase(UdbContext udbContext, UdbConfiguration GDCConfig, String projectId) {
    mUdbContext = udbContext;
    mConfiguration = GDCConfig;
    mGdcDatasetName = udbContext.getUdbDbName();
    mGdcProjectId = projectId;
  }

  /**
   * Creates an instance of the GDC database UDB.
   *
   * @param udbContext    the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static GDCDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    String udbDbName = udbContext.getUdbDbName();
    if (udbDbName == null || udbDbName.isEmpty()) {
      throw new IllegalArgumentException("GDC database name cannot be empty or null: " + udbDbName);
    }
    String credentialsFilename = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    if (credentialsFilename.isEmpty()) {
      throw new IllegalArgumentException(
          "GOOGLE_APPLICATION_CREDENTIALS environment variable not set");
    }
    try {
      Map<String, String> a = new Gson().fromJson(
          new JsonReader(new FileReader(credentialsFilename)), Map.class);
      String projectId = a.get("project_id");
      return new GDCDatabase(udbContext, configuration, projectId);
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public String getType() {
    return GDCDatabaseFactory.TYPE;
  }

  @Override
  public String getName() {
    return mGdcDatasetName;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    List<String> tableNames = new ArrayList<>();
    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    Page<Table> tables = bigQuery.listTables(mGdcDatasetName);
    for (Table table : tables.iterateAll()) {
      tableNames.add(table.getTableId().getTable());
    }
    return tableNames;
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    Table table = bigQuery.getTable(mGdcDatasetName, tableName);
    Schema schema = GDCUtils.toProtoSchema(table.getDefinition().getSchema());

    Map<String, String> tableParameters = Collections.emptyMap();
    PartitionInfo partitionInfo = PartitionInfo.newBuilder()
        // Database name is not required for glue table, use mGlueDbName
        .setDbName(mGdcDatasetName)
        .setTableName(tableName)
        .addAllDataCols(GDCUtils.toProto(table.getDefinition().getSchema()))
//        .setStorage() // don't know what to do with this one
        .putAllParameters(tableParameters)
        .build();

//        .setStorage(GlueUtils.toProto(table.getStorageDescriptor(), pathTranslator))
//        .putAllParameters(tableParameters)

    Layout layout = Layout.newBuilder()
        .setLayoutType(HiveLayout.TYPE)
        .setLayoutData(partitionInfo.toByteString())
        .build();

    List<UdbPartition> udbPartitions = new ArrayList<>();
    try {
      // table has partitions
      List<String> partitions = bigQuery.listPartitions(
          TableId.of(mGdcProjectId, mGdcDatasetName, tableName));
      partitions.forEach(partition -> System.out.println(partition)); // for debugging
      // TODO(jenoudet): implement table having partitions
      throw new BigQueryException(new IOException("partitions not implemented"));
    } catch (BigQueryException e) {
      // table does not have partitions
      PartitionInfo.Builder pib = PartitionInfo.newBuilder()
          .setDbName(getUdbContext().getDbName())
          .setTableName(tableName)
          .addAllDataCols(GDCUtils.toProto(table.getDefinition().getSchema()))
//          .setStorage(HiveUtils.toProto(table.getSd(), pathTranslator))
          .setPartitionName(tableName)
          .putAllParameters(tableParameters);
      udbPartitions.add(new GDCPartition(
          new HiveLayout(pib.build(), Collections.emptyList())));
    }

    return new GDCTable(tableName,
        schema,
        new ArrayList<>(), // placeholder instead of null
        schema.getColsList(),
        udbPartitions, // placeholder instead of null
        layout
    );
  }

  @Override
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public DatabaseInfo getDatabaseInfo() throws IOException {
    // TODO(jenoudet): not very straightforward translation, might need to be revisited
    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    Dataset dataset = bigQuery.getDataset(mGdcDatasetName);
    String comments = dataset.getDescription();

    return new DatabaseInfo(mGdcProjectId + ":" + mGdcDatasetName,
        mOwnerName, mOwnerType, comments, null);
  }
}
