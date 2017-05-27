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

package alluxio.client;

import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystemContext;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class NettyChannelCloseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private WorkerNetAddress mWorkerNetAddress;

  @Before
  public void before() throws Exception {
    mWorkerNetAddress = mClusterResource.get().getWorkerAddress();
  }

  @Test
  public void closeAsync() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Channel channel = FileSystemContext.INSTANCE.acquireNettyChannel(mWorkerNetAddress);
      Assert.assertTrue(channel.isOpen());
      // Note: If you replace closeChannel with channel.close(), this test fails with high
      // probability.
      CommonUtils.closeChannel(channel);
      Assert.assertTrue(!channel.isOpen());
      FileSystemContext.INSTANCE.releaseNettyChannel(mWorkerNetAddress, channel);
    }
  }

  @Test
  public void closeSync() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Channel channel = FileSystemContext.INSTANCE.acquireNettyChannel(mWorkerNetAddress);
      Assert.assertTrue(channel.isOpen());
      // Note: If you replace closeChannel with channel.close(), this test fails with high
      // probability.
      CommonUtils.closeChannelSync(channel);
      Assert.assertTrue(!channel.isOpen());
      FileSystemContext.INSTANCE.releaseNettyChannel(mWorkerNetAddress, channel);
    }
  }
}
