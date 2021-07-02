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

package alluxio.client.table;

import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.worker.file.FileSystemMasterClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

/**
 * Integration tests for the Table Master Client.
 */
public final class TableMasterClientTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH").build();
  private FileSystem mFileSystem = null;
  private FileSystemMasterClient mFSMasterClient;
  private TableMasterClient mTableMasterClient;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    MasterClientContext context = MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build();
    mTableMasterClient = new RetryHandlingTableMasterClient(context);
    mFSMasterClient = new FileSystemMasterClient(context);
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
    mTableMasterClient.close();
  }

  // TODO(david): write tests
}
