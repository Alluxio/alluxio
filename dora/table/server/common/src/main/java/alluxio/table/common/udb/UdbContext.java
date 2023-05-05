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

package alluxio.table.common.udb;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.table.common.CatalogPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The context for the udb.
 */
public class UdbContext {
  private static final Logger LOG = LoggerFactory.getLogger(UdbContext.class);

  private final UnderDatabaseRegistry mUdbRegistry;
  private final FileSystem mFileSystem;

  /** The udb type. */
  private final String mType;
  /** The connection uri for the udb. */
  private final String mConnectionUri;
  /** The name of the database in the udb. */
  private final String mUdbDbName;
  /** The name of the database in Alluxio. */
  private final String mDbName;

  /**
   * Creates an instance.
   *
   * @param udbRegistry the udb registry
   * @param fileSystem the alluxio fs client
   * @param type the db type
   * @param connectionUri the connection uri for the udb
   * @param udbDbName name of the database in the udb
   * @param dbName name of the database in Alluxio
   */
  public UdbContext(UnderDatabaseRegistry udbRegistry, FileSystem fileSystem, String type,
      String connectionUri, String udbDbName, String dbName) {
    mUdbRegistry = udbRegistry;
    mFileSystem = fileSystem;
    mType = type;
    mConnectionUri = connectionUri;
    mUdbDbName = udbDbName;
    mDbName = dbName;
  }

  /**
   * @return the db name in Alluxio
   */
  public String getDbName() {
    return mDbName;
  }

  /**
   * @return the alluxio fs client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the udb registry
   */
  public UnderDatabaseRegistry getUdbRegistry() {
    return mUdbRegistry;
  }

  /**
   * @return the connection uri for the udb
   */
  public String getConnectionUri() {
    return mConnectionUri;
  }

  /**
   * @return the db name in Udb
   */
  public String getUdbDbName() {
    return mUdbDbName;
  }

  /**
   * @param tableName the table name
   * @return the AlluxioURI for the table location for the specified table name
   */
  public AlluxioURI getTableLocation(String tableName) {
    return CatalogPathUtils.getTablePathUdb(mDbName, tableName, mType);
  }
}
