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
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.util.CommonUtils;

/**
 * Local Tachyon cluster with multiple master for unit tests.
 */
public class MasterFaultToleranceTest {
  private final int BLOCK_SIZE = 30;

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
    mLocalTachyonClusterMultiMaster = new LocalTachyonClusterMultiMaster(10000, 5);
    mLocalTachyonClusterMultiMaster.start();
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
  }

  @Test
  public void faultTest() throws IOException {
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

    for (int kills = 1; kills <= 3; kills ++) {
      Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
      CommonUtils.sleepMs(null, 1500);
      mTfs = mLocalTachyonClusterMultiMaster.getClient();
      files = mTfs.ls(Constants.PATH_SEPARATOR, true);
      Assert.assertEquals(clients + 1, files.size());
      Assert.assertEquals(Constants.PATH_SEPARATOR, files.get(0));
      for (int k = 0; k < clients; k ++) {
        Assert.assertEquals(Constants.PATH_SEPARATOR + k, files.get(k + 1));
      }
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
