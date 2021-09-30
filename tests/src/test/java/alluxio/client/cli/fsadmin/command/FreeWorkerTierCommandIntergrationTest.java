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

package alluxio.client.cli.fsadmin.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.grpc.CreateFilePOptions;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;

import org.junit.Test;

import java.io.IOException;

public class FreeWorkerTierCommandIntergrationTest extends AbstractFsAdminShellTest {
  final String mCOMMAND = "freeWorkerTier";
  final String mFILE1 = "/foobar1";

  public void reset() throws Exception {
    if (!mLocalAlluxioCluster.isStartedWorkers()) {
      mLocalAlluxioCluster.startWorkers();
    }
    if (mLocalAlluxioCluster.getClient().exists(new AlluxioURI(mFILE1))) {
      mLocalAlluxioCluster.getClient().delete(new AlluxioURI(mFILE1));
    }
  }

  @Test
  public void normal() {
    int ret;
    ret = mFsAdminShell.run(mCOMMAND);
    assertEquals(0, ret);
    ret = mFsAdminShell.run(mCOMMAND, "force");
    assertEquals(0, ret);
    ret = mFsAdminShell.run(mCOMMAND, "f");
    assertEquals(0, ret);
  }

  @Test
  public void wrongParameter() {
    int ret;
    ret = mFsAdminShell.run(mCOMMAND, "workers", "-1");
    assertEquals(-1, ret);
  }

  @Test
  public void freeToBePersistedBlock() throws Exception {
    reset();
    // The block that persisted block will not be removed from worker tier
    FileSystemTestUtils.createByteFile(mLocalAlluxioCluster.getClient(), new AlluxioURI(mFILE1),
        CreateFilePOptions.newBuilder().setRecursive(true)
            .setWriteType(WritePType.ASYNC_THROUGH).setPersistenceWaitTime(
            Integer.MAX_VALUE).build(), 10);
    assertEquals(0, mFsAdminShell.run(mCOMMAND));
    assertTrue(isInMemoryTest(mFILE1));
  }

  @Test
  public void freePersistedBlock() throws Exception {
    reset();
    // The block that persisted block will be removed from worker tier
    if (mLocalAlluxioCluster.isStartedWorkers()) {
      FileSystemTestUtils.createByteFile(mLocalAlluxioCluster.getClient(),
          mFILE1, WritePType.CACHE_THROUGH, 10);
      assertEquals(0, mFsAdminShell.run(mCOMMAND));
      assertFalse(isInMemoryTest(mFILE1));
    }
  }

  @Test
  public void freeWithFormattedMaster() throws Exception {
    // The block that persisted block will be removed from worker tier without master metadata
    reset();
    FileSystemTestUtils.createByteFile(mLocalAlluxioCluster.getClient(),
        mFILE1, WritePType.CACHE_THROUGH, 10);
    mLocalAlluxioCluster.formatAndRestartMasters();
    assertEquals(0, mFsAdminShell.run(mCOMMAND));
    assertFalse(isInMemoryTest(mFILE1));
  }

  /**
   * @param path a file path
   * @return whether the file is in memory
   */
  protected boolean isInMemoryTest(String path) throws IOException, AlluxioException {
    return (mLocalAlluxioCluster.getClient().getStatus(
        new AlluxioURI(path)).getInMemoryPercentage() == 100);
  }
}
