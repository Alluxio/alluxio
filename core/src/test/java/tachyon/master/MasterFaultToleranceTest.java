/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.client.TachyonFS;
import tachyon.util.CommonUtils;

/**
 * Local Tachyon cluster with multiple master for unit tests.
 */
public class MasterFaultToleranceTest {
  private final int BLOCK_SIZE = 30;
  private final int MASTERS = 5;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonClusterMultiMaster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.default.block.size.byte", String.valueOf(BLOCK_SIZE));
    System.setProperty("fs.hdfs.impl.disable.cache", "true");
    mLocalTachyonClusterMultiMaster = new LocalTachyonClusterMultiMaster(10000, MASTERS);
    mLocalTachyonClusterMultiMaster.start();
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
  }

  /**
   * Create 10 files in the folder
   * 
   * @param foldername
   *          the folder name to create
   * @param answer
   *          the results, the mapping from file id to file path
   * @throws IOException
   */
  private void faultTestDataCreation(String folderName, List<Pair<Integer, String>> answer)
      throws IOException {
    TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
    if (!tfs.exist(folderName)) {
      tfs.mkdir(folderName);
      answer.add(new Pair<Integer, String>(tfs.getFileId(folderName), folderName));
    }

    for (int k = 0; k < 10; k ++) {
      String path = folderName + Constants.PATH_SEPARATOR + folderName.substring(1) + k;
      answer.add(new Pair<Integer, String>(tfs.createFile(path), path));
    }
  }

  /**
   * Tells if the results can match the answer
   * 
   * @param answer
   *          the correct results
   * @throws IOException
   */
  private void faultTestDataCheck(List<Pair<Integer, String>> answer) throws IOException {
    TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
    List<String> files = tfs.ls(Constants.PATH_SEPARATOR, true);
    Assert.assertEquals(answer.size(), files.size());
    for (int k = 0; k < answer.size(); k ++) {
      Assert.assertEquals(answer.get(k).getSecond(), tfs.getFile(answer.get(k).getFirst())
          .getPath());
      Assert.assertEquals(answer.get(k).getFirst().intValue(),
          tfs.getFileId(answer.get(k).getSecond()));
    }
  }

  @Test
  public void faultTest() throws IOException {
    int clients = 10;
    List<Pair<Integer, String>> answer = new ArrayList<Pair<Integer, String>>();
    answer.add(new Pair<Integer, String>(1, Constants.PATH_SEPARATOR));
    // faultTestDataCreation("/", answer);
    for (int k = 0; k < clients; k ++) {
      faultTestDataCreation("/data" + k, answer);
    }

    faultTestDataCheck(answer);

    for (int kills = 0; kills < 1; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(null, Constants.SECOND_MS * 3);
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
  public void getClientsTest() throws IOException {
    int clients = 10;
    mTfs.createFile("/0", 1024);
    for (int k = 1; k < clients; k ++) {
      TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
      tfs.createFile(Constants.PATH_SEPARATOR + k, 1024);
    }
    List<String> files = mTfs.ls(Constants.PATH_SEPARATOR, true);
    Assert.assertEquals(clients + 1, files.size());
    Assert.assertEquals(Constants.PATH_SEPARATOR, files.get(0));
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals(Constants.PATH_SEPARATOR + k, files.get(k + 1));
    }
  }
}
