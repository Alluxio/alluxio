/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.collections.Pair;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

public class MasterFaultToleranceIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private FileSystem mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonClusterMultiMaster.stop();
    // Reset the master conf.
    MasterContext.getConf().merge(new TachyonConf());
  }

  @Before
  public final void before() throws Exception {
    // TODO(gpang): Implement multi-master cluster as a resource.
    // Reset the master conf.
    MasterContext.getConf().merge(new TachyonConf());
    mLocalTachyonClusterMultiMaster =
        new LocalTachyonClusterMultiMaster(WORKER_CAPACITY_BYTES, MASTERS, BLOCK_SIZE);
    mLocalTachyonClusterMultiMaster.start();
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
  }

  /**
   * Creates 10 files in the folder.
   *
   * @param folderName the folder name to create
   * @param answer the results, the mapping from file id to file path
   * @throws IOException if an error occurs creating the file
   */
  private void faultTestDataCreation(TachyonURI folderName, List<Pair<Long, TachyonURI>> answer)
      throws IOException, TachyonException {
    mTfs.createDirectory(folderName);
    answer.add(new Pair<Long, TachyonURI>(mTfs.getStatus(folderName).getFileId(), folderName));

    for (int k = 0; k < 10; k ++) {
      TachyonURI path =
          new TachyonURI(PathUtils.concatPath(folderName, folderName.toString().substring(1) + k));
      mTfs.createFile(path).close();
      answer.add(new Pair<Long, TachyonURI>(mTfs.getStatus(path).getFileId(), path));
    }
  }

  /**
   * Tells if the results can match the answer.
   *
   * @param answer the correct results
   * @throws IOException if an error occurs opening the file
   */
  private void faultTestDataCheck(List<Pair<Long, TachyonURI>> answer) throws IOException,
      TachyonException {
    List<String> files = TachyonFSTestUtils.listFiles(mTfs, TachyonURI.SEPARATOR);
    Collections.sort(files);
    Assert.assertEquals(answer.size(), files.size());
    for (int k = 0; k < answer.size(); k ++) {
      Assert.assertEquals(answer.get(k).getSecond().toString(),
          mTfs.getStatus(answer.get(k).getSecond()).getPath());
      Assert.assertEquals(answer.get(k).getFirst().longValue(),
          mTfs.getStatus(answer.get(k).getSecond()).getFileId());
    }
  }

  @Test
  public void createFileFaultTest() throws Exception {
    int clients = 10;
    List<Pair<Long, TachyonURI>> answer = Lists.newArrayList();
    for (int k = 0; k < clients; k ++) {
      faultTestDataCreation(new TachyonURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);
      faultTestDataCheck(answer);
      faultTestDataCreation(new TachyonURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void deleteFileFaultTest() throws Exception {
    // Kill leader -> create files -> kill leader -> delete files, repeat.
    List<Pair<Long, TachyonURI>> answer = Lists.newArrayList();
    for (int kills = 0; kills < MASTERS - 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      if (kills % 2 != 0) {
        // Delete files.

        faultTestDataCheck(answer);

        // We can not call mTfs.delete(mTfs.open(new TachyonURI(TachyonURI.SEPARATOR))) because root
        // node can not be deleted.
        for (URIStatus file : mTfs.listStatus(new TachyonURI(TachyonURI.SEPARATOR))) {
          mTfs.delete(new TachyonURI(file.getPath()), new DeleteOptions.Builder()
              .setRecursive(true).build());
        }
        answer.clear();
        faultTestDataCheck(answer);
      } else {
        // Create files.

        Assert.assertEquals(0, answer.size());
        faultTestDataCheck(answer);

        faultTestDataCreation(new TachyonURI(PathUtils.concatPath(
            TachyonURI.SEPARATOR, "data_" + kills)), answer);
        faultTestDataCheck(answer);
      }
    }
  }

  @Test
  public void createFilesTest() throws Exception {
    int clients = 10;
    CreateFileOptions option =
        CreateFileOptions.defaults().setBlockSizeBytes(1024).setWriteType(WriteType.THROUGH);
    for (int k = 0; k < clients; k ++) {
      mTfs.createFile(new TachyonURI(TachyonURI.SEPARATOR + k), option).close();
    }
    List<String> files = TachyonFSTestUtils.listFiles(mTfs, TachyonURI.SEPARATOR);
    Assert.assertEquals(clients, files.size());
    Collections.sort(files);
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals(TachyonURI.SEPARATOR + k, files.get(k));
    }
  }

  @Test
  public void killStandbyTest() throws Exception {
    // If standby masters are killed(or node failure), current leader should not be affected and the
    // cluster should run properly.

    int leaderIndex = mLocalTachyonClusterMultiMaster.getLeaderIndex();
    Assert.assertNotEquals(-1, leaderIndex);

    List<Pair<Long, TachyonURI>> answer = Lists.newArrayList();
    for (int k = 0; k < 5; k ++) {
      faultTestDataCreation(new TachyonURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killStandby());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // Leader should not change.
      Assert.assertEquals(leaderIndex, mLocalTachyonClusterMultiMaster.getLeaderIndex());
      // Cluster should still work.
      faultTestDataCheck(answer);
      faultTestDataCreation(new TachyonURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void workerReRegisterTest() throws Exception {
    Assert.assertEquals(WORKER_CAPACITY_BYTES, TachyonBlockStore.get().getCapacityBytes());

    List<Pair<Long, TachyonURI>> emptyAnswer = Lists.newArrayList();
    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // TODO(cc) Why this test fail without this line? [TACHYON-970]
      faultTestDataCheck(emptyAnswer);

      // If worker is successfully re-registered, the capacity bytes should not change.
      Assert.assertEquals(WORKER_CAPACITY_BYTES, TachyonBlockStore.get().getCapacityBytes());
    }
  }
}
