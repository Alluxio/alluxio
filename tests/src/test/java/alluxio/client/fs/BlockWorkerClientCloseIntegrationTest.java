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
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class BlockWorkerClientCloseIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private WorkerNetAddress mWorkerNetAddress;

  @Before
  public void before() throws Exception {
    mWorkerNetAddress = mClusterResource.get().getWorkerAddress();
  }

  @Test
  public void close() throws Exception {
    for (int i = 0; i < 1000; i++) {
      BlockWorkerClient client = FileSystemContext.get().acquireBlockWorkerClient(mWorkerNetAddress);
      Assert.assertFalse(client.isShutdown());
      client.close();
      Assert.assertTrue(client.isShutdown());
      FileSystemContext.get().releaseBlockWorkerClient(mWorkerNetAddress, client);
    }
  }
}
