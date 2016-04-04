/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for free command.
 */
public class FreeCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void freeTest() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("free", "/testFile");
    Configuration configuration = mLocalAlluxioCluster.getMasterConf();
    CommonUtils.sleepMs(configuration.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertFalse(
        mFileSystem.getStatus(new AlluxioURI("/testFile")).getInMemoryPercentage() == 100);
  }

  @Test
  public void freeWildCardTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    Configuration configuration = mLocalAlluxioCluster.getMasterConf();

    int ret = mFsShell.run("free", "/testWild*/foo/*");
    CommonUtils.sleepMs(null,
        configuration.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/foobar4"));

    ret = mFsShell.run("free", "/testWild*/*/");
    CommonUtils.sleepMs(null,
        configuration.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foobar4"));
  }
}
