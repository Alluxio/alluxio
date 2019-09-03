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

import alluxio.Constants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.GrpcService;
import alluxio.grpc.Schema;
import alluxio.grpc.ServiceType;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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
    mCatalog = new AlluxioCatalog();
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
  public boolean createDatabase(String database, CatalogConfiguration configuration)
      throws IOException {
    // TODO(gpang): should type just be an argument?
    if (configuration.get(CatalogProperty.DB_TYPE).isEmpty()) {
      throw new IOException(
          "The database type is not configured. Please set property: " + CatalogProperty.DB_TYPE
              .getName());
    }
    return mCatalog
        .createDatabase(configuration.get(CatalogProperty.DB_TYPE), database, configuration);
  }

  @Override
  public Table createTable(String dbName, String tableName, Schema schema) throws IOException {
    return mCatalog.createTable(dbName, tableName, schema);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws IOException {
    return mCatalog.getTable(dbName, tableName);
  }

  @Override
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName)
      throws IOException {
    // TODO(gpang): revisit api
    return mCatalog.getStatistics(dbName, tableName);
  }

  @Override
  public List<String> getDataFiles(String dbName, String tableName) throws IOException {
    // TODO(gpang): revisit api
    return new ArrayList<>(mCatalog.getStatistics(dbName, tableName).keySet());
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
