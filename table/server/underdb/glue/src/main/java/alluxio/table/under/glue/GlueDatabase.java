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

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.PathTranslator;
import alluxio.table.common.udb.UdbBypassSpec;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UdbUtils;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.AWSGlueException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GlueEncryptionException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.ValidationException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Glue database implementation.
 */
public class GlueDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(GlueDatabase.class);

  private final UdbContext mUdbContext;
  private final AWSGlueAsync mGlueClient;
  private final UdbConfiguration mGlueConfiguration;
  /** the name of the glue db. */
  private final String mGlueDbName;

  /** the owner name of glue database, which is an fake place holder. */
  private final String mOwnerName = "PUBLIC_OWNER";
  private final alluxio.grpc.table.PrincipalType mOwnerType = alluxio.grpc.table.PrincipalType.ROLE;

  @VisibleForTesting
  protected GlueDatabase(UdbContext udbContext, UdbConfiguration glueConfig, String glueDbName) {
    mUdbContext = udbContext;
    mGlueConfiguration = glueConfig;
    mGlueClient = createAsyncGlueClient(glueConfig);
    mGlueDbName = glueDbName;
  }

  /**
   * Create an instance of the Glue database UDB.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static GlueDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    String glueDbName = udbContext.getUdbDbName();
    if (glueDbName == null || glueDbName.isEmpty()) {
      throw new IllegalArgumentException(
          "Glue database name cannot be empty: " + glueDbName);
    } else if (configuration.get(Property.GLUE_REGION) == null) {
      throw new IllegalArgumentException("GlueUdb Error: Please setup aws region.");
    }

    return new GlueDatabase(udbContext, configuration, glueDbName);
  }

  @Override
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public DatabaseInfo getDatabaseInfo() throws IOException {
    try {
      GetDatabaseRequest dbRequest = new GetDatabaseRequest()
          .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
          .withName(mGlueDbName);
      GetDatabaseResult dbResult = mGlueClient.getDatabase(dbRequest);
      Database glueDatabase = dbResult.getDatabase();
      // Glue database location, description and parameters could be null
      String glueDbLocation = glueDatabase.getLocationUri() == null
          ? "" : glueDatabase.getLocationUri();
      String glueDbDescription = glueDatabase.getDescription() == null
          ? "" : glueDatabase.getDescription();
      Map<String, String> glueParameters = new HashMap<>();
      if (glueDatabase.getParameters() != null) {
        glueParameters.putAll(glueDatabase.getParameters());
      }
      return new DatabaseInfo(
          glueDbLocation,
          mOwnerName,
          mOwnerType,
          glueDbDescription,
          glueParameters);
    } catch (EntityNotFoundException e) {
      throw new IOException("Cannot find glue database: " + mGlueDbName
          + "Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID)
          + ". " + e.getMessage(), e);
    }
  }

  /**
   * This method allows user to test udb glue client with remote glue server.
   *
   * @param config udbconfiguration
   * @return glue async client
   */
  protected static AWSGlueAsync createAsyncGlueClient(UdbConfiguration config) {
    ClientConfiguration clientConfig = new ClientConfiguration()
        .withMaxConnections(config.getInt(Property.MAX_GLUE_CONNECTION));

    if (!config.get(Property.AWS_PROXY_HOST).isEmpty()) {
      clientConfig.withProxyProtocol(getProtocol(config.get(Property.AWS_PROXY_PROTOCOL)))
          .withProxyHost(config.get(Property.AWS_PROXY_HOST))
          .withProxyPort(config.getInt(Property.AWS_PROXY_PORT))
          .withProxyUsername(config.get(Property.AWS_PROXY_USER_NAME))
          .withProxyPassword(config.get(Property.AWS_PROXY_PASSWORD));
    }

    AWSGlueAsyncClientBuilder asyncClientBuilder = AWSGlueAsyncClientBuilder
        .standard()
        .withClientConfiguration(clientConfig);

    if (!config.get(Property.GLUE_REGION).isEmpty()) {
      LOG.info("Set Glue region: {}.", config.get(Property.GLUE_REGION));
      asyncClientBuilder.setRegion(config.get(Property.GLUE_REGION));
    } else {
      LOG.warn("GlueDatabase: Please setup the AWS region.");
    }

    asyncClientBuilder.setCredentials(getAWSCredentialsProvider(config));

    return asyncClientBuilder.build();
  }

  private static AWSCredentialsProvider getAWSCredentialsProvider(UdbConfiguration config) {
    //TODO(shouwei): add compelete authentication method for glue udb
    if (!config.get(Property.AWS_GLUE_ACCESS_KEY).isEmpty()
        && !config.get(Property.AWS_GLUE_SECRET_KEY).isEmpty()) {
      return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          config.get(Property.AWS_GLUE_ACCESS_KEY),
          config.get(Property.AWS_GLUE_SECRET_KEY)));
    }
    return DefaultAWSCredentialsProviderChain.getInstance();
  }

  private static Protocol getProtocol(String protocol) {
    if (protocol.equals("HTTP")) {
      return Protocol.HTTP;
    } else if (protocol.equals("HTTPS")) {
      return Protocol.HTTPS;
    } else {
      LOG.warn("Invalid protocol type {}."
          + "Avaiable proxy protocol type HTTP and HTTPS.", protocol);
    }
    return null;
  }

  @Override
  public String getType() {
    return GlueDatabaseFactory.TYPE;
  }

  @Override
  public String getName() {
    return mGlueDbName;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    try {
      String nextToken = null;
      List<String> tableNames = new ArrayList<>();
      do {
        GetTablesRequest tablesRequest =
            new GetTablesRequest()
                .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
                .withDatabaseName(mGlueDbName)
                .withNextToken(nextToken);
        GetTablesResult tablesResult = mGlueClient.getTables(tablesRequest);
        tablesResult.getTableList().forEach(table -> tableNames.add(table.getName()));
        nextToken = tablesResult.getNextToken();
      } while (nextToken != null);
      return tableNames;
    } catch (EntityNotFoundException e) {
      throw new IOException("Failed to get glue tables: " + e.getMessage()
          + " in Database: " + mGlueDbName
          + "; with Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID) + ".", e);
    }
  }

  @VisibleForTesting
  private PathTranslator mountAlluxioPaths(Table table, List<Partition> partitions,
      UdbBypassSpec bypassSpec)
      throws IOException {
    String tableName = table.getName();
    AlluxioURI ufsUri;
    AlluxioURI alluxioUri = mUdbContext.getTableLocation(tableName);
    String glueUfsUri = table.getStorageDescriptor().getLocation();

    try {
      PathTranslator pathTranslator = new PathTranslator();
      if (bypassSpec.hasFullTable(tableName)) {
        pathTranslator.addMapping(glueUfsUri, glueUfsUri);
        return pathTranslator;
      }
      ufsUri = new AlluxioURI(table.getStorageDescriptor().getLocation());
      pathTranslator.addMapping(
          UdbUtils.mountAlluxioPath(
              tableName,
              ufsUri,
              alluxioUri,
              mUdbContext,
              mGlueConfiguration),
          glueUfsUri);

      for (Partition partition : partitions) {
        AlluxioURI partitionUri;
        String partitionName;
        if (partition.getStorageDescriptor() != null
            && partition.getStorageDescriptor().getLocation() != null
            && ufsUri.isAncestorOf(
            partitionUri = new AlluxioURI(
                partition.getStorageDescriptor().getLocation()))) {
          glueUfsUri = partition.getStorageDescriptor().getLocation();
          partitionName = partition.getValues().toString();
          try {
            partitionName = GlueUtils.makePartitionName(
                table.getPartitionKeys(),
                partition.getValues());
          } catch (IOException e) {
            LOG.warn("Error making partition name for table {},"
                    + " partition {} in database {} with CatalogID {}.",
                tableName,
                partition.getValues().toString(),
                mGlueDbName,
                mGlueConfiguration.get(Property.CATALOG_ID));
          }
          if (bypassSpec.hasPartition(tableName, partitionName)) {
            pathTranslator.addMapping(partitionUri.getPath(), partitionUri.getPath());
            continue;
          }
          alluxioUri = new AlluxioURI(
              PathUtils.concatPath(
                  mUdbContext.getTableLocation(tableName).getPath(),
                  partitionName));
          // mount partition path if it is not already mounted as part of the table path mount
          pathTranslator
              .addMapping(
                  UdbUtils.mountAlluxioPath(
                      tableName,
                      partitionUri,
                      alluxioUri,
                      mUdbContext,
                      mGlueConfiguration),
                  glueUfsUri);
        }
      }
      return pathTranslator;
    } catch (AlluxioException e) {
      throw new IOException(
          "Failed to mount table location. tableName: " + tableName
              + " glueUfsLocation: " + glueUfsUri
              + " AlluxioLocation: " + alluxioUri + " error: " + e.getMessage(), e);
    }
  }

  private List<ColumnStatisticsInfo> getTableColumnStatistics(String dbName, String tableName,
      GetColumnStatisticsForTableRequest getColumnStatisticsForTableRequest) {
    // TODO(shouwei): Add Async support for table column statistics
    try {
      return getClient().getColumnStatisticsForTable(getColumnStatisticsForTableRequest)
          .getColumnStatisticsList().stream().map(GlueUtils::toProto).collect(Collectors.toList());
    } catch (AmazonClientException e) {
      LOG.warn("Cannot get the table column statistics info for table {}.{} with error {}.",
          dbName, tableName, e.toString());
    }
    return Collections.emptyList();
  }

  private List<ColumnStatisticsInfo> getPartitionColumnStatistics(String dbName, String tableName,
      GetColumnStatisticsForPartitionRequest getColumnStatisticsForPartitionRequest) {
    // TODO(shouwei): Add Async support for partition column statistics
    try {
      List<ColumnStatisticsInfo> partColumnStatistic = getClient()
          .getColumnStatisticsForPartition(getColumnStatisticsForPartitionRequest)
          .getColumnStatisticsList().stream().map(GlueUtils::toProto).collect(Collectors.toList());
      return partColumnStatistic;
    } catch (AmazonClientException e) {
      LOG.warn("Cannot get the partition column statistics info for table {}.{} with error {}.",
          dbName, tableName, e.toString());
    }
    return Collections.emptyList();
  }

  @Override
  public UdbTable getTable(String tableName, UdbBypassSpec bypassSpec) throws IOException {
    Table table;
    List<Partition> partitions;
    try {
      GetTableRequest tableRequest = new GetTableRequest()
          .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
          .withDatabaseName(mGlueDbName)
          .withName(tableName);
      table = getClient().getTable(tableRequest).getTable();

      partitions = batchGetPartitions(getClient(), tableName);
      PathTranslator pathTranslator = mountAlluxioPaths(table, partitions, bypassSpec);

      List<Column> partitionColumns;
      if (table.getPartitionKeys() == null) {
        partitionColumns = Collections.emptyList();
      } else {
        partitionColumns = table.getPartitionKeys();
      }

      // Get table parameters
      Map<String, String> tableParameters = table.getParameters() == null
          ? Collections.emptyMap() : table.getParameters();

      // Get column statistics info for table
      List<String> columnNames = table.getStorageDescriptor()
          .getColumns().stream().map(Column::getName).collect(Collectors.toList());
      GetColumnStatisticsForTableRequest getColumnStatisticsForTableRequest =
          new GetColumnStatisticsForTableRequest()
              .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
              .withDatabaseName(mGlueDbName)
              .withTableName(tableName)
              .withColumnNames(columnNames);
      List<ColumnStatisticsInfo> columnStatisticsTableData = new ArrayList<>();
      if (mGlueConfiguration.getBoolean(Property.TABLE_COLUMN_STATISTICS_ENABLE)) {
        columnStatisticsTableData = getTableColumnStatistics(
            mGlueDbName, tableName, getColumnStatisticsForTableRequest);
      }

      // Get column statistics info for partitions
      // potential expensive call
      Map<String, List<ColumnStatisticsInfo>> statsMap = new HashMap<>();
      if (mGlueConfiguration.getBoolean(Property.PARTITION_COLUMN_STATISTICS_ENABLE)) {
        for (Partition partition : partitions) {
          List<String> partitionValue = partition.getValues();
          if (partitionValue != null) {
            GetColumnStatisticsForPartitionRequest getColumnStatisticsForPartitionRequest =
                new GetColumnStatisticsForPartitionRequest()
                    .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
                    .withDatabaseName(mGlueDbName)
                    .withTableName(tableName)
                    .withColumnNames(columnNames)
                    .withPartitionValues(partitionValue);
            String partName = GlueUtils.makePartitionName(partitionColumns, partition.getValues());
            statsMap.put(partName, getPartitionColumnStatistics(
                mGlueDbName, tableName, getColumnStatisticsForPartitionRequest));
          }
        }
      }

      PartitionInfo partitionInfo = PartitionInfo.newBuilder()
          // Database name is not required for glue table, use mGlueDbName
          .setDbName(mGlueDbName)
          .setTableName(tableName)
          .addAllDataCols(GlueUtils.toProto(table.getStorageDescriptor().getColumns()))
          .setStorage(GlueUtils.toProto(table.getStorageDescriptor(), pathTranslator))
          .putAllParameters(tableParameters)
          .build();

      Layout layout = Layout.newBuilder()
          .setLayoutType(HiveLayout.TYPE)
          .setLayoutData(partitionInfo.toByteString())
          .build();

      List<UdbPartition> udbPartitions = new ArrayList<>();
      if (partitionColumns.isEmpty()) {
        PartitionInfo.Builder partitionInfoBuilder = PartitionInfo.newBuilder()
            .setDbName(mGlueDbName)
            .setTableName(tableName)
            .addAllDataCols(GlueUtils.toProto(table.getStorageDescriptor().getColumns()))
            .setStorage(GlueUtils.toProto(table.getStorageDescriptor(), pathTranslator))
            .setPartitionName(tableName)
            .putAllParameters(tableParameters);
        udbPartitions.add(new GluePartition(
            new HiveLayout(partitionInfoBuilder.build(), Collections.emptyList())));
      } else {
        for (Partition partition : partitions) {
          String partName = GlueUtils.makePartitionName(partitionColumns, partition.getValues());
          PartitionInfo.Builder partitionInfoBuilder = PartitionInfo.newBuilder()
              .setDbName(mGlueDbName)
              .setTableName(tableName)
              .addAllDataCols(GlueUtils.toProto(partition.getStorageDescriptor().getColumns()))
              .setStorage(GlueUtils.toProto(partition.getStorageDescriptor(), pathTranslator))
              .setPartitionName(partName)
              .putAllParameters(partition.getParameters() == null
                  ? Collections.emptyMap() : partition.getParameters());
          if (partition.getValues() != null) {
            partitionInfoBuilder.addAllValues(partition.getValues());
          }
          udbPartitions.add(new GluePartition(new HiveLayout(partitionInfoBuilder.build(),
              statsMap.getOrDefault(partName, Collections.emptyList()))));
        }
      }

      return new GlueTable(this,
          pathTranslator,
          tableName,
          GlueUtils.toProtoSchema(table.getStorageDescriptor().getColumns()),
          columnStatisticsTableData,
          // Glue does not provide FieldSchema from API directly
          // Get FieldSchema from partition keys
          GlueUtils.toProto(table.getPartitionKeys()),
          udbPartitions,
          layout,
          table);
    } catch (EntityNotFoundException e) {
      throw new NotFoundException("Table " + tableName
          + " does not exist in Database: " + mGlueDbName
          + "; Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID)
          + ".", e);
    } catch (ValidationException e) {
      throw new IOException("Failed to get table: " + tableName
          + " in Database: " + mGlueDbName
          + "; Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID)
          + " with validation error: " + e.getMessage(), e);
    } catch (GlueEncryptionException e) {
      throw new IOException("Failed to get table: " + tableName
          + " in Database: " + mGlueDbName
          + "; Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID)
          + " error: " + e.getMessage(), e);
    }
  }

  private List<Partition> batchGetPartitions(AWSGlueAsync glueClient, String tableName)
      throws IOException {
    // TODO(shouwei): make getPartition multi-thread to accelerate the large table fetching
    List<Partition> partitions = new ArrayList<>();
    String nextToken = null;
    try {
      do {
        GetPartitionsRequest getPartitionsRequest =
            new GetPartitionsRequest()
                .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
                .withDatabaseName(mGlueDbName)
                .withTableName(tableName)
                .withMaxResults(mGlueConfiguration.getInt(Property.MAX_GLUE_FETCH_PARTITIONS))
                .withNextToken(nextToken);
        GetPartitionsResult getPartitionsResult = glueClient.getPartitions(getPartitionsRequest);
        partitions.addAll(getPartitionsResult.getPartitions());
        nextToken = getPartitionsResult.getNextToken();
        LOG.debug("Glue table {}.{} adding {} batch partitions with total {} partitions.",
            mGlueDbName, tableName, getPartitionsResult.getPartitions().size(), partitions.size());
      } while (nextToken != null);

      if (partitions != null) {
        LOG.info("Glue table {}.{} has {} partitions.",
            mGlueDbName, tableName, partitions.size());
        if (LOG.isDebugEnabled()) {
          partitions.stream().forEach(partition ->
              LOG.debug("Glue table {}.{} with partition: {}.",
                  partition.getDatabaseName(), tableName, partition.toString()));
        }
      }
      return partitions;
    } catch (AWSGlueException e) {
      throw new IOException("Cannot get partition information for table: " + tableName
          + " in Database: " + mGlueDbName
          + "; Catalog ID: " + mGlueConfiguration.get(Property.CATALOG_ID)
          + ". error: " + e.getMessage(), e);
    }
  }

  /**
   * Get Glue Client.
   *
   * @return async glue client
   */
  public AWSGlueAsync getClient() {
    return mGlueClient;
  }
}
