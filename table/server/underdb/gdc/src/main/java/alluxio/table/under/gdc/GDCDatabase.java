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

package alluxio.table.under.gdc;

import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import com.google.api.gax.paging.Page;
import com.google.common.annotations.VisibleForTesting;
import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Google Data Catalog database implementation.
 */
public class GDCDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(GDCDatabase.class);

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;
  private final String mGdcTableName;

  @VisibleForTesting
  protected GDCDatabase(UdbContext udbContext, UdbConfiguration GDCConfig) {
    mUdbContext = udbContext;
    mConfiguration = GDCConfig;
    mGdcTableName = udbContext.getUdbDbName();
  }

  /**
   * Creates an instance of the GDC database UDB.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static GDCDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    return new GDCDatabase(udbContext, configuration);
  }

  @Override
  public String getType() {
    return GDCDatabaseFactory.TYPE;
  }

  @Override
  public String getName() {
    return mGdcTableName;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    List<String> tableNames = new ArrayList<>();
    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    Page<Table> tables = bigQuery.listTables(mGdcTableName);
    for (Table table : tables.iterateAll()) {
      tableNames.add(table.getTableId().getTable());
    }
    return tableNames;
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    return null;
  }

  @Override
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public DatabaseInfo getDatabaseInfo() throws IOException {
    return null;
  }
}
