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

import alluxio.Server;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This catalog master manages catalogs metadata information.
 */
public class DefaultCatalogMaster implements CatalogMaster {
  @Override
  public List<String> getAllDatabases() {
    return null;
  }

  @Override
  public Database getDatabase(String dbName) {
    return null;
  }

  @Override
  public List<String> getAllTables(String databaseName) {
    return null;
  }

  @Override
  public void createDatabase(Database database) {
  }

  @Override
  public void createTable(Table table) {
  }

  @Override
  public List<FieldSchema> getFields(String databaseName, String tableName) {
    return null;
  }

  @Override
  public JournalContext createJournalContext() throws UnavailableException {
    return null;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.CATALOG_MASTER_CLIENT_SERVICE,
        new GrpcService(new CatalogMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public void start(Boolean options) throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void close() throws IOException {
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
