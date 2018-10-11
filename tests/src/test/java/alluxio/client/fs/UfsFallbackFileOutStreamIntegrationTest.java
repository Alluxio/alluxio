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

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
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
 *
 */
@RunWith(Parameterized.class)
public class UfsFallbackFileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  protected static final int WORKER_MEMORY_SIZE = 1500;
  protected static final int BUFFER_BYTES = 100;

  @Override
  protected LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    return new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.WORKER_FILE_BUFFER_SIZE, BUFFER_BYTES) // initial buffer for worker
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_MEMORY_SIZE)
        .setProperty(PropertyKey.USER_FILE_UFS_TIER_ENABLED, true)
        .setProperty(PropertyKey.WORKER_TIERED_STORE_RESERVER_ENABLED, false)
        .build();
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

  @Test
  public void shortCircuitWrite() throws Exception {
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(mUserFileBufferSize));
        put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, String.valueOf(mBlockSize));
      }
    }).toResource()) {
      AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
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

  @Ignore("Files may be lost due to evicting and committing before file is complete.")
  @Test
  public void nettyWrite() throws Exception {
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(mUserFileBufferSize));
        put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, String.valueOf(mBlockSize));
        put(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, "false");
      }
    }).toResource()) {
      AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
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
