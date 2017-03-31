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

package alluxio.client.block;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.resource.LockBlockResource;
import alluxio.wire.LockBlockResult;
import alluxio.wire.LockBlockResult.LockBlockStatus;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;

/**
 * Unit tests for {@link UnderFileSystemBlockInStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class UnderFileSystemBlockInStreamTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void readFromLocal() throws Exception {
    File blockFile = mFolder.newFile();
    blockFile.createNewFile();
    String clientHostname = "clientHostname";
    try (Closeable c = new ConfigurationRule(
        ImmutableMap.of(PropertyKey.USER_HOSTNAME, clientHostname)).toResource()) {
      long blockId = 0;
      FileSystemContext context = mock(FileSystemContext.class);
      BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
      when(context.createBlockWorkerClient(any(WorkerNetAddress.class)))
          .thenReturn(blockWorkerClient);
      // Mock the lock result to show that the block is locked in Alluxio storage.
      LockBlockResult lockResult =
          new LockBlockResult().setLockBlockStatus(LockBlockStatus.ALLUXIO_BLOCK_LOCKED)
              .setBlockPath(blockFile.getAbsolutePath());
      LockBlockResource lockResource =
          new LockBlockResource(blockWorkerClient, lockResult, blockId);
      when(blockWorkerClient.lockUfsBlock(eq(blockId), any(LockBlockOptions.class)))
          .thenReturn(lockResource);

      // Set the data server hostname to match the client hostname.
      when(blockWorkerClient.getDataServerAddress())
          .thenReturn(InetSocketAddress.createUnresolved(clientHostname, 0));
      BlockInStream stream = UnderFileSystemBlockInStream.create(context, "ufsPath", blockId, 100,
          0, new WorkerNetAddress(), InStreamOptions.defaults());
      // The client hostname matches the worker hostname, so the stream should go to a local file.
      Assert.assertTrue(stream instanceof LocalBlockInStream);

      // Set the data server hostname to not match the client hostname.
      when(blockWorkerClient.getDataServerAddress())
          .thenReturn(InetSocketAddress.createUnresolved("remotehost", 0));
      stream = UnderFileSystemBlockInStream.create(context, "ufsPath", blockId, 100,
          0, new WorkerNetAddress(), InStreamOptions.defaults());

      // The client hostname matches the worker hostname, so the stream should go to a local file.
      Assert.assertFalse(stream instanceof LocalBlockInStream);
    }
  }
}
