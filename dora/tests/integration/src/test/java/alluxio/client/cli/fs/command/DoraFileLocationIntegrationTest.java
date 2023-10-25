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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.cli.fs.AbstractDoraFileSystemShellTest;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Ignore
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "yimin",
    comment = "Bring back but not passed, need to fix.")
public class DoraFileLocationIntegrationTest extends AbstractDoraFileSystemShellTest {

  public DoraFileLocationIntegrationTest() throws IOException {
    super(3);
  }

  @Override
  public void before() throws Exception {
    mLocalAlluxioClusterResource.setProperty(
        PropertyKey.JOB_BATCH_SIZE, 3
    );
    mLocalAlluxioClusterResource.setProperty(
        PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY, "1s"
    );
    super.before();
  }

  @Test
  public void testCommand() throws Exception {
    mTestFolder.newFolder("testRoot");

    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    createByteFileInUfs("/testRoot/testFileA", Constants.MB);
    assertEquals(0, mFsShell.run("load", "/testRoot", "--submit", "--verify"));
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uriA, 100);

    assertEquals(100, mFileSystem.getStatus(uriA).getInAlluxioPercentage());
    assertEquals(0, mFsShell.run("location", "/testRoot/testFileA"));

    DoraCacheFileSystem doraCacheFileSystem = mFileSystem.getDoraCacheFileSystem();
    assert doraCacheFileSystem != null;
    AlluxioURI ufsFullPath = doraCacheFileSystem.convertToUfsPath(uriA);
    String ufsPath = ufsFullPath.toString();
    Set<String> workersThatHaveDataSet = new HashSet<>();
    WorkerNetAddress workerNetAddress = doraCacheFileSystem.getWorkerNetAddress(uriA);
    workersThatHaveDataSet.add(workerNetAddress.getHost());
    assertTrue(mOutput.toString().contains(ufsPath));
    assertTrue(mOutput.toString().contains(workerNetAddress.getHost()));
    assertTrue(mOutput.toString().contains(Boolean.TRUE.toString()));
    assertTrue(mOutput.toString().contains(workersThatHaveDataSet.stream().iterator().next()));
  }
}
