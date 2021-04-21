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

import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.DataCatalogClient.SearchCatalogPagedResponse;
import com.google.cloud.datacatalog.v1.SearchCatalogResult;
import com.google.cloud.datacatalog.v1.SearchCatalogRequest.Scope;
import com.google.common.annotations.VisibleForTesting;
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
  private final String mGdcProjectID;

  @VisibleForTesting
  protected GDCDatabase(UdbContext udbContext, UdbConfiguration GDCConfig) {
    mUdbContext = udbContext;
    mConfiguration = GDCConfig;
    mGdcProjectID = udbContext.getUdbDbName();
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
    return mGdcProjectID;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    Scope scope = Scope.newBuilder().addIncludeProjectIds(mGdcProjectID).build();
    List<String> tableNames = new ArrayList<>();
    try (DataCatalogClient dataCatalogClient = DataCatalogClient.create()) {
      String query = "type=table";
      SearchCatalogPagedResponse response = dataCatalogClient.searchCatalog(scope, query);
      for (SearchCatalogResult result : response.iterateAll()) {
        String[] decomposedPath = result.getLinkedResource().split("/");
        tableNames.add(decomposedPath[decomposedPath.length - 1]);
      }
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
