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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.util.CommonUtils;

/**
 * Local Tachyon cluster with multiple master for unit tests.
 */
public class MasterFaultToleranceIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonClusterMultiMaster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonClusterMultiMaster =
        new LocalTachyonClusterMultiMaster(10000, MASTERS, BLOCK_SIZE);
    mLocalTachyonClusterMultiMaster.start();
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
  }

  /**
   * Create 10 files in the folder
   *
   * @param folderName the folder name to create
   * @param answer the results, the mapping from file id to file path
   * @throws IOException
   */
  private void faultTestDataCreation(TachyonURI folderName, List<Pair<Integer, TachyonURI>> answer)
      throws IOException {
    TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
    if (!tfs.exist(folderName)) {
      tfs.mkdir(folderName);
      answer.add(new Pair<Integer, TachyonURI>(tfs.getFileId(folderName), folderName));
    }

    for (int k = 0; k < 10; k ++) {
      TachyonURI path =
          new TachyonURI(
              folderName + TachyonURI.SEPARATOR + folderName.toString().substring(1) + k);
      answer.add(new Pair<Integer, TachyonURI>(tfs.createFile(path), path));
    }
  }

  /**
   * Tells if the results can match the answer
   *
   * @param answer the correct results
   * @throws IOException
   */
  private void faultTestDataCheck(List<Pair<Integer, TachyonURI>> answer) throws IOException {
    TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
    List<String> files = TachyonFSTestUtils.listFiles(tfs, TachyonURI.SEPARATOR);
    Assert.assertEquals(answer.size(), files.size());
    for (int k = 0; k < answer.size(); k ++) {
      Assert.assertEquals(answer.get(k).getSecond().toString(),
          tfs.getFile(answer.get(k).getFirst()).getPath());
      Assert.assertEquals(answer.get(k).getFirst().intValue(),
          tfs.getFileId(answer.get(k).getSecond()));
    }
  }

  @Ignore
  @Test
  public void faultTest() throws IOException {
    int clients = 10;
    List<Pair<Integer, TachyonURI>> answer = new ArrayList<Pair<Integer, TachyonURI>>();
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

  @Ignore
  @Test
  public void getClientsTest() throws IOException {
    int clients = 10;
    mTfs.createFile(new TachyonURI("/0"), 1024);
    for (int k = 1; k < clients; k ++) {
      TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
      tfs.createFile(new TachyonURI(TachyonURI.SEPARATOR + k), 1024);
    }
    List<String> files = TachyonFSTestUtils.listFiles(mTfs, TachyonURI.SEPARATOR);
    Assert.assertEquals(clients, files.size());
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals(TachyonURI.SEPARATOR + k, files.get(k));
    }
  }
}
