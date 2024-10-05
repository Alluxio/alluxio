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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of utility methods for catalog paths.
 *
 * Catalog paths for tables look like:
 * /&lt;catalog base dir&gt;/&lt;dbName1&gt;/tables/&lt;tableName1&gt;/&lt;udbType&gt;/...
 *                                                  /_internal_/...
 *                                     /&lt;tableName2&gt;/&lt;udbType&gt;/...
 *                                                  /_internal_/...
 * /&lt;catalog base dir&gt;/&lt;dbName2&gt;/tables/&lt;tableName3&gt;/&lt;udbType&gt;/...
 *                                                  /_internal_/...
 *
 * Catalog paths for fragments look like:
 * /<catalog base dir>/<dbName2>/fragments/<fragment1>/...
 *                                        /<fragment2>/...
 * A fragment of a database is a UFS location that stores a part of the tables
 * of the database. A UDB may be split into several fragments.
 */
public class CatalogPathUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogPathUtils.class);
  private static final String TABLES_ROOT = "tables";
  private static final String INTERNAL_ROOT = "_internal_";
  private static final String FRAGMENTS_ROOT = "fragments";

  private CatalogPathUtils() {} // prevent instantiation

  /**
   * @param dbName the database name
   * @param tableName the table name
   * @param udbType the udb type
   * @return the AlluxioURI for the path for the specified table
   */
  public static AlluxioURI getTablePathUdb(String dbName, String tableName, String udbType) {
    return new AlluxioURI(PathUtils
        .concatPath(Configuration.get(PropertyKey.TABLE_CATALOG_PATH), dbName, TABLES_ROOT,
            tableName, udbType));
  }

  /**
   * @param dbName the database name
   * @param tableName the table name
   * @return the AlluxioURI for the path for the specified table, for internal data
   */
  public static AlluxioURI getTablePathInternal(String dbName, String tableName) {
    return new AlluxioURI(PathUtils
        .concatPath(Configuration.get(PropertyKey.TABLE_CATALOG_PATH), dbName, TABLES_ROOT,
            tableName, INTERNAL_ROOT));
  }

  /**
   * @param dbName the database name
   * @param fragmentUri fragment ufs uri
   * @return the AlluxioURI for the fragments path of the database
   */
  public static AlluxioURI getFragmentsPath(String dbName, AlluxioURI fragmentUri) {
    return new AlluxioURI(PathUtils.concatPath(
        ServerConfiguration.get(PropertyKey.TABLE_CATALOG_PATH), dbName, FRAGMENTS_ROOT,
        escapeFragmentUfsPath(fragmentUri)));
  }

  /**
   * Escape a ufs URI to be embedded in a fragment path.
   *
   * @param fragmentUfsUri fragment URI to escape
   * @return escaped fragment URI
   */
  public static String escapeFragmentUfsPath(AlluxioURI fragmentUfsUri) {
    String scheme = fragmentUfsUri.getScheme();
    scheme = scheme == null || scheme.isEmpty() ? "none" : scheme;
    String authority = fragmentUfsUri.getAuthority().toString();
    authority = authority == null || authority.isEmpty() ? "none" : authority;
    String escapedFragmentPath = String.format("%s_%s", scheme, authority)
        .replaceAll(":", "_")
        .replaceAll("/", "_");
    return escapedFragmentPath;
  }
}
