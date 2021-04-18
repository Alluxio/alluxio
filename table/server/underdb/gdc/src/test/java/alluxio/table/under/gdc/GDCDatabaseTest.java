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

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GDCDatabaseTest {

  private static final String DB_NAME = "alluxio-internal";

  @Test
  public void getTableNames() throws IOException {
    UdbConfiguration mUdbConfiguration = new UdbConfiguration(ImmutableMap.of());
    UdbContext mUdbContext = new UdbContext(null, null, "glue", "null", DB_NAME, DB_NAME);
    GDCDatabase db = GDCDatabase.create(mUdbContext, mUdbConfiguration);

    List<String> tableNames = db.getTableNames();
    Assert.assertNotEquals(0, tableNames.size());
    Assert.assertTrue(tableNames.contains("test"));
  }
}
