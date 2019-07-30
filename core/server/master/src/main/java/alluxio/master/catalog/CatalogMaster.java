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

import alluxio.master.Master;
//TODO: replace these classes with our own version of Database and Table classes
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;


import java.util.List;

/**
 * Interface of the catalog master that manages the catalog metadata
 */
public interface CatalogMaster extends Master {
  List<String> getAllDatabases();
  Database getDatabase(String dbName);
  List<String> getAllTables(String databaseName);
  void createDatabase(Database database);
  void createTable(Table table);
  List<FieldSchema> getFields(String databaseName, String tableName);
}