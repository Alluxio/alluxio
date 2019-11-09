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

package alluxio.table.common.transform;

import alluxio.AlluxioURI;
import alluxio.table.common.CatalogPathUtils;
import alluxio.util.CommonUtils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * The context for generating transformation plans.
 */
public class TransformContext {
  private static final FastDateFormat DATE_FORMAT =
      FastDateFormat.getInstance("yyyyMMdd-HHmmss-SSS", TimeZone.getDefault(), Locale.getDefault());

  private final String mDbName;
  private final String mTableName;
  private final String mIdentifier;

  /**
   * Creates an instance.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param identifier the identifier for this transformation
   */
  public TransformContext(String dbName, String tableName, String identifier) {
    mDbName = dbName;
    mTableName = tableName;
    mIdentifier = identifier;
  }

  /**
   * @return a newly generated path to a transformed partition
   */
  public AlluxioURI generateTransformedPath() {
    String random =
        String.format("%s-%s", DATE_FORMAT.format(new Date()), CommonUtils.randomAlphaNumString(5));
    // append a random identifier, to avoid collisions
    return CatalogPathUtils.getTablePathInternal(mDbName, mTableName).join(mIdentifier)
        .join(random);
  }
}
