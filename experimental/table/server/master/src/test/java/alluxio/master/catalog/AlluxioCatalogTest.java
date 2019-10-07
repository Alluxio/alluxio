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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class AlluxioCatalogTest {

  private AlluxioCatalog mCatalog;

  @Before
  public void before() {
    mCatalog = new AlluxioCatalog();
  }

  @Test
  public void attachDb() throws Exception {
    String dbName = "testdb";
    mCatalog.attachDatabase(NoopUdbFactory.TYPE, dbName,
        new CatalogConfiguration(Collections.emptyMap()));
    List<String> dbs = mCatalog.getAllDatabases();
    assertEquals(1, dbs.size());
    assertEquals(dbName, dbs.get(0));
  }
}
