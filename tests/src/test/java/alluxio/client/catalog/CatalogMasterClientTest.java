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

package alluxio.client.catalog;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

/**
 * Integration tests for the Catalog Master Client.
 */
public final class CatalogMasterClientTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH").build();
  private FileSystem mFileSystem = null;
  private FileSystemMasterClient mFSMasterClient;
  private CatalogMasterClient mCatalogMasterClient;
  private SetAttributePOptions mSetPinned;
  private SetAttributePOptions mUnsetPinned;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    MasterClientContext context = MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build();
    mCatalogMasterClient = new RetryHandlingCatalogMasterClient(context);
    mFSMasterClient = new FileSystemMasterClient(context);
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
    mCatalogMasterClient.close();
  }

  /**
   * Tests catalog service table metadata operation
   */
  @Test
  public void tableOps() throws Exception {
    Assert.assertEquals(0, mFSMasterClient.getPinList().size());
    List<String> dbs = mCatalogMasterClient.getAllDatabases();

    Assert.assertEquals( 0, dbs.size());
  }

}
