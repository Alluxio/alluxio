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
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.util.CommonUtils;

/**
 * Test master journal for crashes. Assert that test can replay the editlog and reproduce the
 * correct state. Test both the single master(tachyon) and multi masters(tachyon-ft) mode.
 */
public class JournalCrashIntegrationTest {

  /**
   * Hold a client and keep creating files/tables.
   */
  class ClientThread implements Runnable {
    /** The number of successfully created files/tables. */
    public int mSuccessNum = 0;

    private final int mOpType;  //0:create file; 1:create raw table.
    private final TachyonFS mTfs;

    public ClientThread(int opType, TachyonFS tfs) {
      mOpType = opType;
      mTfs = tfs;
    }

    @Override
    public void run() {
      try {
        while (true) {
          if (mOpType == 0) {
            if (mTfs.createFile(new TachyonURI(TEST_FILE_DIR + mSuccessNum)) == -1) {
              break;
            }
          } else if (mOpType == 1) {
            if (mTfs.createRawTable(new TachyonURI(TEST_TABLE_DIR + mSuccessNum), 1) == -1) {
              break;
            }
          }
          mSuccessNum ++;
          CommonUtils.sleepMs(null, 100);
        }
      } catch (Exception e) {
        // Something crashed. Stop the thread.
      }
    }
  }

  private static final int TEST_BLOCK_SIZE = 128;
  private static final String TEST_FILE_DIR = "/files/";
  private static final int TEST_MASTERS = 3;
  private static final String TEST_TABLE_DIR = "/tables/";
  private static final long TEST_TIME_MS = Constants.SECOND_MS;

  private ClientThread mCreateFileThread = null;
  private ClientThread mCreateTableThread = null;
  private final ExecutorService mExecutorsForClient = Executors.newFixedThreadPool(2);
  private final ExecutorService mExecutorsForMaster = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf =  null;

  @After
  public final void after() throws Exception {
    mExecutorsForClient.shutdown();
    mExecutorsForMaster.shutdown();
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws Exception {
    System.setProperty("fs.hdfs.impl.disable.cache", "true");
  }

  /**
   * Check if the correct state can be reproduced.
   */
  private void crashTestUtil(int successFiles, int successTables) throws IOException,
      InvalidPathException, FileDoesNotExistException, TableDoesNotExistException {
    String masterJournal = mMasterTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
    Journal journal = new Journal(masterJournal, "image.data", "log.data", mMasterTachyonConf);
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal, mExecutorsForMaster,
        mMasterTachyonConf);
    info.init();

    Assert.assertEquals(successFiles, info.listFiles(new TachyonURI(TEST_FILE_DIR), false).size());
    for (int f = 0; f < successFiles; f ++) {
      Assert.assertTrue(info.getFileId(new TachyonURI(TEST_FILE_DIR + f)) != -1);
    }
    Assert.assertEquals(successTables,
        info.listFiles(new TachyonURI(TEST_TABLE_DIR), false).size());
    for (int t = 0; t < successTables; t ++) {
      Assert.assertTrue(info.getRawTableId(new TachyonURI(TEST_TABLE_DIR + t)) != -1);
    }
    info.stop();
  }

  private LocalTachyonClusterMultiMaster setupMultiMasterCluster() throws IOException {
    LocalTachyonClusterMultiMaster ret =
        new LocalTachyonClusterMultiMaster(100, TEST_MASTERS, TEST_BLOCK_SIZE);
    ret.start();
    mMasterTachyonConf = ret.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(0, ret.getClient());
    mCreateTableThread = new ClientThread(1, ret.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    mExecutorsForClient.submit(mCreateTableThread);
    return ret;
  }

  private LocalTachyonCluster setupSingleMasterCluster() throws IOException {
    LocalTachyonCluster ret = new LocalTachyonCluster(100, 100, TEST_BLOCK_SIZE);
    ret.start();
    mMasterTachyonConf = ret.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(0, ret.getClient());
    mCreateTableThread = new ClientThread(1, ret.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    mExecutorsForClient.submit(mCreateTableThread);
    return ret;
  }

  @Test
  public void singleMasterJournalCrashIntegrationTest() throws Exception {
    LocalTachyonCluster cluster = setupSingleMasterCluster();
    CommonUtils.sleepMs(null, TEST_TIME_MS);
    cluster.stopTFS();
    CommonUtils.sleepMs(null, TEST_TIME_MS);
    crashTestUtil(mCreateFileThread.mSuccessNum, mCreateTableThread.mSuccessNum);
    cluster.stop();
  }

  @Test
  public void multiMasterJournalCrashIntegrationTest()  throws Exception {
    LocalTachyonClusterMultiMaster cluster = setupMultiMasterCluster();
    for (int kills = 0; kills < TEST_MASTERS; kills ++) {
      CommonUtils.sleepMs(null, TEST_TIME_MS);
      Assert.assertTrue(cluster.killLeader());
    }
    cluster.stopTFS();
    CommonUtils.sleepMs(null, TEST_TIME_MS);
    crashTestUtil(mCreateFileThread.mSuccessNum, mCreateTableThread.mSuccessNum);
    cluster.stop();
  }
}
