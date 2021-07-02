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

package alluxio.table.common;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.PathUtils;

import org.junit.Test;

public class CatalogPathUtilsTest {

  @Test
  public void tablePathUdb() {
    String dbName = "dbName";
    String tableName = "tableName";
    String udbType = "udbType";
    AlluxioURI path = CatalogPathUtils.getTablePathUdb(dbName, tableName, udbType);

    assertEquals(path.getPath(), PathUtils.concatPath(ServerConfiguration.global().get(
        PropertyKey.TABLE_CATALOG_PATH), dbName, "tables", tableName, udbType));
  }

  @Test
  public void tablePathInternal() {
    String dbName = "dbName";
    String tableName = "tableName";
    AlluxioURI path = CatalogPathUtils.getTablePathInternal(dbName, tableName);

    assertEquals(path.getPath(), PathUtils.concatPath(ServerConfiguration.global().get(
        PropertyKey.TABLE_CATALOG_PATH), dbName, "tables", tableName, "_internal_"));
  }
}
