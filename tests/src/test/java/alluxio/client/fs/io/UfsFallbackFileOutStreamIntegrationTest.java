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

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

/**
 * Integration tests for {@link FileOutStream} of under storage type being async
 * persist.
 */
@RunWith(Parameterized.class)
public class UfsFallbackFileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallyScheduleEviction =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_SPACE_RESERVER);

  protected static final int WORKER_MEMORY_SIZE = 1500;

  @Override
  protected void customizeClusterResource(LocalAlluxioClusterResource.Builder resource) {
    resource.setProperty(PropertyKey.WORKER_RAMDISK_SIZE, WORKER_MEMORY_SIZE)
        .setProperty(PropertyKey.USER_FILE_UFS_TIER_ENABLED, true)
        .setProperty(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH, "1.0");
  }

  // varying the client side configuration
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        // File size fits the available worker memory capacity, no UFS fallback is expected.
        {WORKER_MEMORY_SIZE, 1000, 100},
        // File size exceeds the available worker memory capacity, and thus triggering the UFS
        // fallback on client on first write of second block
        {WORKER_MEMORY_SIZE + 1, 1000, 100},
        // The initial request size exceeds the available worker memory capacity and thus triggering
        // the UFS fallback on client on first write of first block.
        {WORKER_MEMORY_SIZE + 1, 1000, WORKER_MEMORY_SIZE + 1},
        // The initial request size exceeds the available worker memory capacity and thus triggering
        // the UFS fallback on client on first write of last block.
        {WORKER_MEMORY_SIZE + 1, 100, 100},
    });
  }

  @Parameterized.Parameter
  public int mFileLength;
  @Parameterized.Parameter(1)
  public int mBlockSize;
  @Parameterized.Parameter(2)
  public int mUserFileBufferSize;

  @Ignore
  @Test
  public void shortCircuitWrite() throws Exception {

    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, String.valueOf(mBlockSize));
      }
    }, ServerConfiguration.global()).toResource()) {
      FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());
      AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
      CreateFilePOptions op = CreateFilePOptions.newBuilder()
          .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build();
      writeIncreasingBytesToFile(fs, filePath, mFileLength, op);

      CommonUtils.sleepMs(1);
      // check the file is completed but not persisted
      URIStatus status = mFileSystem.getStatus(filePath);
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

      status = mFileSystem.getStatus(filePath);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

      checkFileInAlluxio(filePath, mFileLength);
      checkFileInUnderStorage(filePath, mFileLength);
    }
  }

  @Ignore("Files may be lost due to evicting and committing before file is complete.")
  @Test
  public void grpcWrite() throws Exception {
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(mUserFileBufferSize));
        put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, String.valueOf(mBlockSize));
        put(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, "false");
      }
    }, ServerConfiguration.global()).toResource()) {
      AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
      CreateFilePOptions op = CreateFilePOptions.newBuilder()
          .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build();
      writeIncreasingBytesToFile(filePath, mFileLength, op);

      CommonUtils.sleepMs(1);
      // check the file is completed but not persisted
      URIStatus status = mFileSystem.getStatus(filePath);
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

      status = mFileSystem.getStatus(filePath);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

      checkFileInAlluxio(filePath, mFileLength);
      checkFileInUnderStorage(filePath, mFileLength);
    }
  }
}
