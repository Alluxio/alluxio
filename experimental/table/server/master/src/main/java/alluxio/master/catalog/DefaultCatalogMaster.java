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

package alluxio.master.catalog;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.ColumnStatisticsList;
import alluxio.grpc.catalog.Constraint;
import alluxio.grpc.catalog.Partition;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This catalog master manages catalogs metadata information.
 */
public class DefaultCatalogMaster extends CoreMaster implements CatalogMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCatalogMaster.class);
  private static final Set<Class<? extends Server>> DEPS = ImmutableSet.of(FileSystemMaster.class);

  private final AlluxioCatalog mCatalog;

  /**
   * Constructor for DefaultCatalogMaster.
   *
   * @param context core master context
   */
  public DefaultCatalogMaster(CoreMasterContext context) {
    super(context, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.CATALOG_MASTER_NAME));
    mCatalog = new AlluxioCatalog();
  }

  @Override
  public boolean attachDatabase(String dbName, String dbType, CatalogConfiguration configuration)
      throws IOException {
    return mCatalog.attachDatabase(dbType, dbName, configuration);
  }

  @Override
  public List<String> getAllDatabases() throws IOException {
    return mCatalog.getAllDatabases();
  }

  @Override
  public List<String> getAllTables(String databaseName) throws IOException {
    return mCatalog.getAllTables(databaseName);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws IOException {
    return mCatalog.getTable(dbName, tableName);
  }

  @Override
  public List<ColumnStatisticsInfo> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws IOException {
    return mCatalog.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public List<Partition> readTable(String dbName, String tableName,
      Constraint constraint) throws IOException {
    return mCatalog.readTable(dbName, tableName, constraint);
  }

  @Override
  public void transformTable(String dbName, String tableName, String type, String newTableLocation)
      throws IOException {
    Table table = mCatalog.getTable(dbName, tableName);
    AlluxioURI tableLocation = new AlluxioURI(table.getUdbTable().getBaseLocation());
    AlluxioURI transformedTableLocation = new AlluxioURI(tableLocation.getScheme(),
        tableLocation.getAuthority(), newTableLocation);
    table.getUdbTable().updateLocation(transformedTableLocation.toString());
    for (alluxio.master.catalog.Partition partition : table.getPartitions()) {
      AlluxioURI transformedPartitionLocation = transformedTableLocation.join(
          partition.getLayout().getSpec());
      partition.transformLayout(type, transformedPartitionLocation.toString());
    }
  }

  @Override
  public Map<String, ColumnStatisticsList> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNamesList, List<String> colNamesList) throws IOException {
    return mCatalog.getPartitionColumnStatistics(dbName, tableName, partNamesList, colNamesList);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.CATALOG_MASTER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.CATALOG_MASTER_CLIENT_SERVICE,
        new GrpcService(new CatalogMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  @Override
  public void stop() throws IOException {
    super.stop();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    return false;
  }

  @Override
  public void resetState() {
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return Collections.emptyIterator();
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.CATALOG_SERVICE_MASTER;
  }
}
