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

import alluxio.clock.SystemClock;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.experimental.ProtoUtils;
import alluxio.grpc.ColumnStatistics;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.GrpcService;
import alluxio.grpc.Schema;
import alluxio.grpc.ServiceType;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This catalog master manages catalogs metadata information.
 */
public class DefaultCatalogMaster extends CoreMaster implements CatalogMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCatalogMaster.class);

  private final AlluxioCatalog mCatalog;
  private final UnderFileSystem mUfs;

  /**
   * Constructor for DefaultCatalogMaster.
   *
   * @param context core master context
   */
  public DefaultCatalogMaster(CoreMasterContext context) {
    super(context, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.CATALOG_MASTER_NAME));
    if (URIUtils.isLocalFilesystem(ServerConfiguration
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))) {
      mUfs = UnderFileSystem.Factory
          .create("/", UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    } else {
      mUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    }
    mCatalog = new AlluxioCatalog(mUfs);
  }

  @Override
  public List<String> getAllDatabases() {
    return mCatalog.getAllDatabases();
  }

  @Override
  public List<String> getAllTables(String databaseName) {
    return mCatalog.getAllTables(databaseName);
  }

  @Override
  public boolean createDatabase(String database) {
    return mCatalog.createDatabase(database);
  }

  @Override
  public Table createTable(String dbName, String tableName, Schema schema) {
    TableIdentifier id = TableIdentifier.of(dbName, tableName);
    Table table  = mCatalog.createTable(id, ProtoUtils.fromProto(schema));
    return table;
  }

  @Override
  public Table getTable(String dbName, String tableName) {
    return mCatalog.getTable(TableIdentifier.of(dbName, tableName));
  }

  @Override
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName) {
    Table table = getTable(dbName, tableName);
    Map<String, FileStatistics> map = new HashMap<>();
    for (FileScanTask task : table.newScan().planFiles()) {
      Map<Integer, Long> columnValueCount = task.file().valueCounts();
      Map<Integer, ColumnStatistics> statisticsMap = new HashMap<>();
      for (Map.Entry<Integer, Long> entry : columnValueCount.entrySet()) {
        statisticsMap.put(entry.getKey(),
            ColumnStatistics.newBuilder().setRecordCount(entry.getValue()).build());
      }
      map.put(task.file().path().toString(),
          FileStatistics.newBuilder().putAllColumn(statisticsMap).build());
    }
    return map;
  }

  @Override
  public List<String> getDataFiles(String dbName, String tableName) {
    Table table = getTable(dbName, tableName);
    List<String> list = new ArrayList<>();
    for (FileScanTask task : table.newScan().planFiles()) {

      list.add(task.file().path().toString());
    }
    return list;
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
    return null;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return null;
  }
}
