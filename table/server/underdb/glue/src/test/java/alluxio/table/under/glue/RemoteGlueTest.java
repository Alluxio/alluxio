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

package alluxio.table.under.glue;

import static org.junit.Assert.assertEquals;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RemoteGlueTest {

  private static final String DB_NAME = "test";
  private static final Map<String, String> CONF = new HashMap<>();

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConfiguration;
  private GlueDatabase mGlueDatabase;
  private AWSGlueAsync mGlueClient;

  private static final String AWS_ACCESS_KEY_ID = "<PUT_YOUR_ACCESS_KEY_ID_HERE>";
  private static final String AWS_SECRET_KEY = "<PUT_YOUR_SECRET_KEY_HERE>";
  private static final String CATALOG_ID = "<PUT_YOUR_CATALOG_ID_HERE>";
  private static final String AWS_REGION = "<PUT_GLUE_REGION_HERE>";

  @Ignore
  @Before
  /**
   * Integration test with remote glue service.
   */
  public void connect() {
    CONF.put("aws.accesskey", AWS_ACCESS_KEY_ID);
    CONF.put("aws.secretkey", AWS_SECRET_KEY);
    CONF.put("aws.region", AWS_REGION);
    CONF.put("aws.catalog.id", CATALOG_ID);
    mUdbConfiguration = new UdbConfiguration(CONF);
    mGlueDatabase = new GlueDatabase(mUdbContext, mUdbConfiguration, DB_NAME);
    mGlueClient = mGlueDatabase.getClient();
  }

  @Ignore
  @Test
  public void getDatabase() {
    GetDatabaseRequest dbRequest = new GetDatabaseRequest()
        .withCatalogId(CATALOG_ID)
        .withName(DB_NAME);
    assertEquals(DB_NAME, mGlueDatabase.getClient().getDatabase(dbRequest).getDatabase().getName());
  }

  @Ignore
  @Test
  public void getTables() throws IOException {
    for (String tableName : mGlueDatabase.getTableNames()) {
      System.out.println("Table Names: " + tableName + ".");
    }

    System.out.println("Table counts: " + mGlueDatabase.getTableNames().size());
  }
}
