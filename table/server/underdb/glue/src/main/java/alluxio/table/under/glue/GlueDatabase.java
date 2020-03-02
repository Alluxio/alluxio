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
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.layout.GlueLayoutFactory;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.under.glue.util.PathTranslator;
import alluxio.util.io.PathUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetTableRequest;

import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
      throw new IllegalArgumentException("Glue database name cannot be empty: " + glueDbName);
    }

    return new GlueDatabase(udbContext, configuration, glueDbName);
  }

  @VisibleForTesting
  protected static AWSGlueAsync createAsyncGlueClient(UdbConfiguration config) {
    ClientConfiguration clientConfig = new ClientConfiguration()
        .withMaxConnections(config.getInt(Property.MAX_GLUE_CONNECTION));
    AWSGlueAsyncClientBuilder asyncClientBuilder = AWSGlueAsyncClientBuilder
        .standard()
        .withClientConfiguration(clientConfig);

    if (config.get(Property.GLUE_REGION).isEmpty()) {
      asyncClientBuilder.setRegion(config.get(Property.GLUE_REGION));
    }

    asyncClientBuilder.setCredentials(getAWSCredentialsProvider(config));

    return asyncClientBuilder.build();
  }

  private static AWSCredentialsProvider getAWSCredentialsProvider(UdbConfiguration config) {
    if (config.get(Property.AWS_GLUE_ACCESS_KEY).isEmpty()
        && config.get(Property.AWS_GLUE_SECRET_KEY).isEmpty()) {
      return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
          config.get(Property.AWS_GLUE_ACCESS_KEY),
          config.get(Property.AWS_GLUE_SECRET_KEY)));
    }
    return DefaultAWSCredentialsProviderChain.getInstance();
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
      throw new IOException("Failed to get glue tables: " + e.getMessage(), e);
    }
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    Table table;
    try {
      GetTableRequest tableRequest = new GetTableRequest()
          .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
          .withDatabaseName(mGlueDbName)
          .withName(tableName);
      table = getClient().getTable(tableRequest).getTable();

      List<Partition> partitions = batchGetPartitions(getClient(), tableName);
      PathTranslator pathTranslator = mountAlluxioPaths(table, partitions);

      // Glue does not provide column statistic information
      List<ColumnStatisticsInfo> columnStatisticsData = new ArrayList<>();

      PartitionInfo partitionInfo = PartitionInfo.newBuilder()
          .setDbName(mGlueDbName)
          .setTableName(tableName)
          .addAllDataCols(GlueUtils.toProto(table.getPartitionKeys()))
          .setStorage(GlueUtils.toProto(table.getStorageDescriptor(), pathTranslator))
          .putAllParameters(table.getParameters())
          .build();

      Layout layout = Layout.newBuilder()
          .setLayoutType(GlueLayoutFactory.TYPE)
          .setLayoutData(partitionInfo.toByteString())
          .build();

      return new GlueTable(this,
          pathTranslator,
          tableName,
          GlueUtils.toProtoSchema(table.getPartitionKeys()),
          columnStatisticsData,
          // Glue does not provide FieldSchema from API directly
          // Get FieldSchema from storage description
          GlueUtils.toProto(table.getStorageDescriptor().getColumns()),
          partitions,
          layout,
          table,
          table.getStorageDescriptor().getLocation());
    } catch (EntityNotFoundException e) {
      throw new NotFoundException("Table " + tableName + " does not exist.", e);
    } catch (AlluxioException e) {
      throw new IOException("Cannot mount Alluxio path:" + e.getMessage());
    }
  }

  private List<Partition> batchGetPartitions(AWSGlueAsync glueClient, String tableName)
      throws IOException {
    List<Partition> partitions;
    BatchGetPartitionRequest batchGetPartitionRequest =
        new BatchGetPartitionRequest()
        .withCatalogId(mGlueConfiguration.get(Property.CATALOG_ID))
        .withDatabaseName(mGlueDbName)
        .withTableName(tableName);
    partitions = glueClient.batchGetPartition(batchGetPartitionRequest).getPartitions();
    return partitions;
  }

  private String mountAlluxioPath(String tableName, AlluxioURI ufsUri, AlluxioURI tableUri)
      throws IOException, AlluxioException {
    if (Objects.equals(ufsUri.getScheme(), Constants.SCHEME)) {
      // already an alluxio uri, return the alluxio uri
      return ufsUri.toString();
    }
    try {
      tableUri = mUdbContext.getFileSystem().reverseResolve(ufsUri);
      LOG.debug("Trying to mount table {} location {}, but it is already mounted at location {}",
          tableName, ufsUri, tableUri);
      return tableUri.getPath();
    } catch (InvalidPathException e) {
      // ufs path not mounted, continue
    }
    // make sure the parent exists
    mUdbContext.getFileSystem().createDirectory(tableUri.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
    Map<String, String> mountOptionMap = mGlueConfiguration.getMountOption(
        String.format("%s://%s/", ufsUri.getScheme(), ufsUri.getAuthority().toString()));
    MountPOptions.Builder option = MountPOptions.newBuilder();
    for (Map.Entry<String, String> entry : mountOptionMap.entrySet()) {
      if (entry.getKey().equals(UdbConfiguration.READ_ONLY_OPTION)) {
        option.setReadOnly(Boolean.parseBoolean(entry.getValue()));
      } else if (entry.getKey().equals(UdbConfiguration.SHARED_OPTION)) {
        option.setShared(Boolean.parseBoolean(entry.getValue()));
      } else {
        option.putProperties(entry.getKey(), entry.getValue());
      }
    }
    mUdbContext.getFileSystem().mount(tableUri, ufsUri, option.build());

    LOG.info("mounted table {} location {} to Alluxio location {} with mountOption {}",
        tableName, ufsUri, tableUri, option.build());
    return tableUri.getPath();
  }

  private PathTranslator mountAlluxioPaths(Table table, List<Partition> partitions)
      throws IOException, AlluxioException {
    String tableName = table.getName();
    AlluxioURI ufsUri;
    AlluxioURI alluxioUri = mUdbContext.getTableLocation(tableName);
    String glueUfsUri = table.getStorageDescriptor().getLocation();

    try {
      PathTranslator pathTranslator = new PathTranslator();
      ufsUri = new AlluxioURI(table.getStorageDescriptor().getLocation());
      pathTranslator.addMapping(mountAlluxioPath(tableName, ufsUri, alluxioUri), glueUfsUri);

      for (Partition partition : partitions) {
        AlluxioURI partitionUri;
        if (partition.getStorageDescriptor() != null
            && partition.getStorageDescriptor().getLocation() != null
            && ufsUri.isAncestorOf(
                partitionUri = new AlluxioURI(
                    partition.getStorageDescriptor().getLocation()))) {
          glueUfsUri = partition.getStorageDescriptor().getLocation();
          String partitionName = partition.getValues().toString();
          // Glue does not provide makePartName as Hive, use a simple conveter for place holder
          try {
            partitionName = GlueUtils.makePartitionName(table, partition);
          } catch (IOException e) {
            LOG.warn("Error making partition name for table {}, partition {}", tableName,
                partition.getValues().toString());
          }
          alluxioUri = new AlluxioURI(
              PathUtils.concatPath(
                  mUdbContext.getTableLocation(tableName).getPath(),
                  partitionName));
          // mount partition path if it is not already mounted as part of the table path mount
          pathTranslator
              .addMapping(mountAlluxioPath(tableName, partitionUri, alluxioUri), glueUfsUri);
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
      Database glueDatabaseInfo = dbResult.getDatabase();
      String glueDbLocation = glueDatabaseInfo.getLocationUri();
      String glueDbDescription = glueDatabaseInfo.getDescription();
      Map<String, String> glueParameters = glueDatabaseInfo.getParameters();
      return new DatabaseInfo(
          glueDbLocation,
          mOwnerName,
          mOwnerType,
          glueDbDescription,
          glueParameters);
    } catch (EntityNotFoundException e) {
      throw new IOException("Cannot find glue database" + mGlueDbName
          + "." + e.getMessage(), e);
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
