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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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
  }

  @Before
  public final void before() throws Exception {
    // TODO(gpang): Implement multi-master cluster as a resource.
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(MASTERS);
    mMultiMasterLocalAlluxioCluster.initConfiguration();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES);
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    mMultiMasterLocalAlluxioCluster.start();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
  }

  /**
   * Creates 10 files in the folder.
   *
   * @param folderName the folder name to create
   * @param answer the results, the mapping from file id to file path
   */
  private void faultTestDataCreation(AlluxioURI folderName, List<Pair<Long, AlluxioURI>> answer)
      throws IOException, AlluxioException {
    mFileSystem.createDirectory(folderName);
    answer.add(new Pair<>(mFileSystem.getStatus(folderName).getFileId(), folderName));

    for (int k = 0; k < 10; k++) {
      AlluxioURI path =
          new AlluxioURI(PathUtils.concatPath(folderName, folderName.toString().substring(1) + k));
      mFileSystem.createFile(path).close();
      answer.add(new Pair<>(mFileSystem.getStatus(path).getFileId(), path));
    }
  }

  /**
   * Tells if the results can match the answers.
   *
   * @param answers the correct results
   */
  private void faultTestDataCheck(List<Pair<Long, AlluxioURI>> answers) throws IOException,
      AlluxioException {
    List<String> files = FileSystemTestUtils.listFiles(mFileSystem, AlluxioURI.SEPARATOR);
    Collections.sort(files);
    Assert.assertEquals(answers.size(), files.size());
    for (Pair<Long, AlluxioURI> answer : answers) {
      Assert.assertEquals(answer.getSecond().toString(),
          mFileSystem.getStatus(answer.getSecond()).getPath());
      Assert.assertEquals(answer.getFirst().longValue(),
          mFileSystem.getStatus(answer.getSecond()).getFileId());
    }
  }

  @Test
  public void createFileFault() throws Exception {
    int clients = 10;
    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
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
  public void deleteFileFault() throws Exception {
    // Kill leader -> create files -> kill leader -> delete files, repeat.
    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
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
  public void createFiles() throws Exception {
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
  public void killStandby() throws Exception {
    // If standby masters are killed(or node failure), current leader should not be affected and the
    // cluster should run properly.

    int leaderIndex = mMultiMasterLocalAlluxioCluster.getLeaderIndex();
    Assert.assertNotEquals(-1, leaderIndex);

    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
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
  public void workerReRegister() throws Exception {
    AlluxioBlockStore store = new AlluxioBlockStore();
    Assert.assertEquals(WORKER_CAPACITY_BYTES, store.getCapacityBytes());

    List<Pair<Long, AlluxioURI>> emptyAnswer = new ArrayList<>();
    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // TODO(cc) Why this test fail without this line? [ALLUXIO-970]
      faultTestDataCheck(emptyAnswer);

      // If worker is successfully re-registered, the capacity bytes should not change.
      Assert.assertEquals(WORKER_CAPACITY_BYTES, store.getCapacityBytes());
    }
  }
}
