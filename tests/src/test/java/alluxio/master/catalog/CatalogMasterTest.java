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

import alluxio.AlluxioURI;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CatalogMasterTest {

  @Rule
  public  LocalAlluxioClusterResource mMaster =
      new LocalAlluxioClusterResource.Builder().setNumWorkers(0)
      .build();

  private CatalogMaster mCatalogMaster;

  @Before
  public void before() throws Exception {
    mCatalogMaster = mMaster.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(CatalogMaster.class);
    mMaster.get().getClient().createDirectory(new AlluxioURI("/catalog"));
  }

  /**
   * To execute, an externally running hive metastore must be running at thrift://localhost:9083
   *
   * When running this test within intellij, ufs.hadoop.version must be set to 2.7.3 or higher so
   * that the HDFS UFS module properly compiles.
   *
   * @throws Exception
   */
  @Ignore
  @Test
  public void testAttachDb() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("udb-hive.hive.metastore.uris", "thrift://localhost:9083");
    config.put("udb-hive.database-name", "default");
    mCatalogMaster.attachDatabase("alluxiodb", "hive", new CatalogConfiguration(config));
    mCatalogMaster.getAllTables("alluxiodb");
  }
}
