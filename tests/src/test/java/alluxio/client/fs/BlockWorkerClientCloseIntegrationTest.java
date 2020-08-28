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

package alluxio.client.fs;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.CloseableResource;
import alluxio.security.user.TestUserState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class BlockWorkerClientCloseIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private WorkerNetAddress mWorkerNetAddress;
  private FileSystemContext mFsContext;

  @Before
  public void before() throws Exception {
    mWorkerNetAddress = mClusterResource.get().getWorkerAddress();
    mFsContext = FileSystemContext
        .create(new TestUserState("test", ServerConfiguration.global()).getSubject(),
            ServerConfiguration.global());
  }

  @After
  public void after() throws Exception {
    mFsContext.close();
  }

  @Test
  public void close() throws Exception {
    for (int i = 0; i < 1000; i++) {
      CloseableResource<BlockWorkerClient> client = mFsContext
          .acquireBlockWorkerClient(mWorkerNetAddress);
      Assert.assertFalse(client.get().isShutdown());
      client.get().close();
      Assert.assertTrue(client.get().isShutdown());
      client.close();
    }
  }
}
