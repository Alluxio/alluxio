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

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.client.ClientOptions;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

public class MasterFaultToleranceIntegrationTest {
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private TachyonFileSystem mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonClusterMultiMaster.stop();
  }

  @Before
  public final void before() throws Exception {
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
      throws IOException {
    mTfs.mkdirs(folderName);
    answer.add(new Pair<Long, TachyonURI>(mTfs.open(folderName).getFileId(), folderName));

    for (int k = 0; k < 10; k ++) {
      TachyonURI path =
          new TachyonURI(PathUtils.concatPath(folderName, folderName.toString().substring(1) + k));
      mTfs.getOutStream(path, ClientOptions.defaults()).close();
      answer.add(new Pair<Long, TachyonURI>(mTfs.open(path).getFileId(), path));
    }
  }

  /**
   * Tells if the results can match the answer.
   *
   * @param answer the correct results
   * @throws IOException if an error occurs opening the file
   */
  private void faultTestDataCheck(List<Pair<Long, TachyonURI>> answer) throws IOException {
    List<String> files = TachyonFSTestUtils.listFiles(mTfs, TachyonURI.SEPARATOR);
    Collections.sort(files);
    Assert.assertEquals(answer.size(), files.size());
    for (int k = 0; k < answer.size(); k ++) {
      Assert.assertEquals(answer.get(k).getSecond().toString(),
          mTfs.getInfo(new TachyonFile(answer.get(k).getFirst())).getPath());
      Assert.assertEquals(answer.get(k).getFirst().intValue(),
          mTfs.open(answer.get(k).getSecond()).getFileId());
    }
  }

  @Test
  public void faultTest() throws IOException {
    int clients = 10;
    List<Pair<Long, TachyonURI>> answer = Lists.newArrayList();
    for (int k = 0; k < clients; k ++) {
      faultTestDataCreation(new TachyonURI("/data" + k), answer);
    }

    faultTestDataCheck(answer);

    for (int kills = 0; kills < 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(Constants.SECOND_MS * 3);
      faultTestDataCheck(answer);
    }

    for (int kills = 1; kills < MASTERS - 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(null, Constants.SECOND_MS * 3);
      faultTestDataCheck(answer);
      // TODO Add the following line back
      // faultTestDataCreation("/data" + (clients + kills + 1), answer);
    }
  }

  @Test
  public void createFilesTest() throws IOException {
    int clients = 10;
    ClientOptions option = new ClientOptions.Builder(new TachyonConf()).setBlockSize(1024)
        .setUnderStorageType(UnderStorageType.PERSIST).build();
    for (int k = 0; k < clients; k ++) {
      TachyonFileSystem tfs = mLocalTachyonClusterMultiMaster.getClient();
      tfs.getOutStream(new TachyonURI(TachyonURI.SEPARATOR + k), option).close();
    }
    List<String> files = TachyonFSTestUtils.listFiles(mTfs, TachyonURI.SEPARATOR);
    Assert.assertEquals(clients, files.size());
    Collections.sort(files);
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals(TachyonURI.SEPARATOR + k, files.get(k));
    }
  }
}
