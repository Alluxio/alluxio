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

package alluxio.table.under.hive;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Tables;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// TODO(gpang): probably requires a refactor with HiveDatabase class.
/**
 * Class representing a hive database, while utilizing iceberg.
 */
public class HiveDataCatalog extends BaseMetastoreCatalog implements Closeable, Tables {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDataCatalog.class);

  private final Set<String> mDatabases = new HashSet<>();
  private final Map<String, Map<String, Table>> mDbToTables = new HashMap<>();

  private UnderFileSystem mUfs = null;

  /**
   * Creates a new instance.
   *
   * @param fs the ufs instance
   */
  public HiveDataCatalog(UnderFileSystem fs) {
    super();
    mUfs = fs;
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * create a database.
   *
   * @param dbName database name
   * @return true if database successfully created
   */
  public boolean createDatabase(String dbName) {
    boolean newDb = !mDatabases.contains(dbName);
    mDatabases.add(dbName);
    mDbToTables.compute(dbName, (key, tableList) -> {
      Map<String, Table> returnMap;
      if (tableList == null) {
        returnMap = new HashMap<>();
      } else {
        returnMap = tableList;
      }
      return returnMap;
    });
    return newDb;
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
      String location, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
        "Missing database in table identifier: %s", identifier);
    // throws AlreadyExistException if the table already exist
    Table table = super.createTable(identifier, schema, spec, location, properties);
    if (table != null) {
      addTable(identifier, table);
    }
    return table;
  }

  /**
   * Get a table object based on the identifier.
   *
   * @param id identifer of the table
   * @return a table object
   */
  public Table getTable(TableIdentifier id) {
    Preconditions.checkArgument(id.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", id);
    String dbName = id.namespace().level(0);
    String tableName = id.name();
    Map<String, Table> tables = mDbToTables.get(dbName);
    if (tables == null) {
      return null;
    }
    return tables.get(tableName);
  }

  private Table removeTable(TableIdentifier id) {
    Preconditions.checkArgument(id.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", id);
    String dbName = id.namespace().level(0);
    String tableName = id.name();
    Map<String, Table> tables = mDbToTables.get(dbName);
    if (tables == null) {
      return null;
    }
    return tables.remove(tableName);
  }

  private void addTable(TableIdentifier id, Table table) {
    Preconditions.checkArgument(id.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", id);
    Preconditions.checkNotNull(table, "table can not be null");
    String dbName = id.namespace().level(0);
    String tableName = id.name();
    mDatabases.add(dbName);
    mDbToTables.compute(dbName, (key, tableList) -> {
      Map<String, Table> returnMap;
      if (tableList == null) {
        returnMap = new HashMap<>();
      } else {
        returnMap = tableList;
      }
      returnMap.put(tableName, table);
      return returnMap;
    });
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);
    Table table = getTable(identifier);
    if (table != null) {
      return table;
    } else {
      table = super.loadTable(identifier);
      if (table != null) {
        addTable(identifier, table);
      }
      return table;
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);
    String tableName = identifier.name();
    String dbName = identifier.namespace().level(0);
    return new AlluxioTableOperations(mUfs, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String pathPrefix = PathUtils.concatPath(ServerConfiguration.get(
        PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS),
        ServerConfiguration.get(PropertyKey.METADATA_PATH));
    return String.format("%s/%s.db/%s", pathPrefix, tableIdentifier.namespace().levels()[0],
        tableIdentifier.name());
  }

  @Override
  public boolean dropTable(org.apache.iceberg.catalog.TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException("to be implemented");
  }

  @Override
  public boolean dropTable(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
        "Missing database in table identifier: %s", identifier);
    return removeTable(identifier) != null;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkArgument(from.namespace().levels().length == 1,
        "Missing database in table identifier: %s", from);
    Preconditions.checkArgument(to.namespace().levels().length == 1,
        "Missing database in table identifier: %s", to);

    Table table = getTable(from);
    if (table == null) {
      return;
    }

    createTable(to, table.schema(), table.spec(), table.properties());
    dropTable(from);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, Map<String,
      String> properties, String location) {
    Preconditions.checkNotNull(schema, "A table schema is required");
    TableOperations ops = new AlluxioTableOperations(mUfs, location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists at location: " + location);
    } else {
      Map<String, String> tableProps = properties == null ? ImmutableMap.of() : properties;
      PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
      TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, partitionSpec,
          location, tableProps);
      ops.commit(null, metadata);
      return new BaseTable(ops, location);
    }
  }

  @Override
  public Table load(String location) {
    return loadAs(location, TableIdentifier.of("PathTable", location));
  }

  /**
   * Load a table from a specific location.
   *
   * @param location data file location of the table
   * @param identifier identifier of the table
   * @return a table object
   */
  public Table loadAs(String location, TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
        "Missing database in table identifier: %s", identifier);
    TableOperations ops = new AlluxioTableOperations(mUfs, location,
        identifier.namespace().level(0), identifier.name());

    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    } else {
      return new BaseTable(ops, location);
    }
  }

  /**
   * Get all databases.
   *
   * @return a list of all database names
   */
  public List<String> getAllDatabases() {
    return new ArrayList<>(mDatabases);
  }

  /**
   * Get a list of tables in a database.
   *
   * @param databaseName database name
   * @return a list of table names in the database
   */
  public List<String> getAllTables(String databaseName) {
    Map<String, Table> tables = mDbToTables.getOrDefault(databaseName, new HashMap<>());
    return tables.keySet().stream().map(Object::toString)
        .collect(Collectors.toList());
  }
}
