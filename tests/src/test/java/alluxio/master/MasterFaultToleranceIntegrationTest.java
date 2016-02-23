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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class MasterFaultToleranceIntegrationTest {

  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster = null;
  private FileSystem mFileSystem = null;

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();
    // Reset the master conf.
    MasterContext.getConf().merge(new Configuration());
  }

  @Before
  public final void before() throws Exception {
    // TODO(gpang): Implement multi-master cluster as a resource.
    // Reset the master conf.
    MasterContext.getConf().merge(new Configuration());
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(WORKER_CAPACITY_BYTES, MASTERS, BLOCK_SIZE);
    mMultiMasterLocalAlluxioCluster.start();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
  }

  /**
   * Creates 10 files in the folder.
   *
   * @param folderName the folder name to create
   * @param answer the results, the mapping from file id to file path
   * @throws IOException if an error occurs creating the file
   */
  private void faultTestDataCreation(AlluxioURI folderName, List<Pair<Long, AlluxioURI>> answer)
      throws IOException, AlluxioException {
    mFileSystem.createDirectory(folderName);
    answer
        .add(new Pair<Long, AlluxioURI>(mFileSystem.getStatus(folderName).getFileId(), folderName));

    for (int k = 0; k < 10; k++) {
      AlluxioURI path =
          new AlluxioURI(PathUtils.concatPath(folderName, folderName.toString().substring(1) + k));
      mFileSystem.createFile(path).close();
      answer.add(new Pair<Long, AlluxioURI>(mFileSystem.getStatus(path).getFileId(), path));
    }
  }

  /**
   * Tells if the results can match the answer.
   *
   * @param answer the correct results
   * @throws IOException if an error occurs opening the file
   */
  private void faultTestDataCheck(List<Pair<Long, AlluxioURI>> answer) throws IOException,
      AlluxioException {
    List<String> files = FileSystemTestUtils.listFiles(mFileSystem, AlluxioURI.SEPARATOR);
    Collections.sort(files);
    Assert.assertEquals(answer.size(), files.size());
    for (int k = 0; k < answer.size(); k++) {
      Assert.assertEquals(answer.get(k).getSecond().toString(),
          mFileSystem.getStatus(answer.get(k).getSecond()).getPath());
      Assert.assertEquals(answer.get(k).getFirst().longValue(),
          mFileSystem.getStatus(answer.get(k).getSecond()).getFileId());
    }
  }

  @Test
  public void createFileFaultTest() throws Exception {
    int clients = 10;
    List<Pair<Long, AlluxioURI>> answer = Lists.newArrayList();
    for (int k = 0; k < clients; k++) {
      faultTestDataCreation(new AlluxioURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);
      faultTestDataCheck(answer);
      faultTestDataCreation(new AlluxioURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void deleteFileFaultTest() throws Exception {
    // Kill leader -> create files -> kill leader -> delete files, repeat.
    List<Pair<Long, AlluxioURI>> answer = Lists.newArrayList();
    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      if (kills % 2 != 0) {
        // Delete files.

        faultTestDataCheck(answer);

        // We can not call mFileSystem.delete(mFileSystem.open(new
        // AlluxioURI(AlluxioURI.SEPARATOR))) because root node can not be deleted.
        for (URIStatus file : mFileSystem.listStatus(new AlluxioURI(AlluxioURI.SEPARATOR))) {
          mFileSystem.delete(new AlluxioURI(file.getPath()),
              DeleteOptions.defaults().setRecursive(true));
        }
        answer.clear();
        faultTestDataCheck(answer);
      } else {
        // Create files.

        Assert.assertEquals(0, answer.size());
        faultTestDataCheck(answer);

        faultTestDataCreation(new AlluxioURI(PathUtils.concatPath(
            AlluxioURI.SEPARATOR, "data_" + kills)), answer);
        faultTestDataCheck(answer);
      }
    }
  }

  @Test
  public void createFilesTest() throws Exception {
    int clients = 10;
    CreateFileOptions option =
        CreateFileOptions.defaults().setBlockSizeBytes(1024).setWriteType(WriteType.THROUGH);
    for (int k = 0; k < clients; k++) {
      mFileSystem.createFile(new AlluxioURI(AlluxioURI.SEPARATOR + k), option).close();
    }
    List<String> files = FileSystemTestUtils.listFiles(mFileSystem, AlluxioURI.SEPARATOR);
    Assert.assertEquals(clients, files.size());
    Collections.sort(files);
    for (int k = 0; k < clients; k++) {
      Assert.assertEquals(AlluxioURI.SEPARATOR + k, files.get(k));
    }
  }

  @Test
  public void killStandbyTest() throws Exception {
    // If standby masters are killed(or node failure), current leader should not be affected and the
    // cluster should run properly.

    int leaderIndex = mMultiMasterLocalAlluxioCluster.getLeaderIndex();
    Assert.assertNotEquals(-1, leaderIndex);

    List<Pair<Long, AlluxioURI>> answer = Lists.newArrayList();
    for (int k = 0; k < 5; k++) {
      faultTestDataCreation(new AlluxioURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.killStandby());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // Leader should not change.
      Assert.assertEquals(leaderIndex, mMultiMasterLocalAlluxioCluster.getLeaderIndex());
      // Cluster should still work.
      faultTestDataCheck(answer);
      faultTestDataCreation(new AlluxioURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void workerReRegisterTest() throws Exception {
    Assert.assertEquals(WORKER_CAPACITY_BYTES, AlluxioBlockStore.get().getCapacityBytes());

    List<Pair<Long, AlluxioURI>> emptyAnswer = Lists.newArrayList();
    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // TODO(cc) Why this test fail without this line? [ALLUXIO-970]
      faultTestDataCheck(emptyAnswer);

      // If worker is successfully re-registered, the capacity bytes should not change.
      Assert.assertEquals(WORKER_CAPACITY_BYTES, AlluxioBlockStore.get().getCapacityBytes());
    }
  }
}
