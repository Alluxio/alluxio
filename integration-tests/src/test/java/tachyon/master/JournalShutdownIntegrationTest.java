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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TableDoesNotExistException;
import tachyon.master.file.FileSystemMaster;
import tachyon.util.CommonUtils;

/**
 * Test master journal for cluster terminating. Assert that test can replay the editlog and
 * reproduce the correct state. Test both the single master(tachyon) and multi masters(tachyon-ft).
 */
public class JournalShutdownIntegrationTest {

  /**
   * Hold a client and keep creating files/tables.
   */
  class ClientThread implements Runnable {
    /** The number of successfully created files/tables. */
    private int mSuccessNum = 0;

    private final int mOpType; // 0:create file; 1:create raw table.
    private final TachyonFileSystem mTfs;

    public ClientThread(int opType, TachyonFileSystem tfs) {
      mOpType = opType;
      mTfs = tfs;
    }

    public int getSuccessNum() {
      return mSuccessNum;
    }

    /**
     * Keep creating files/tables until something crashes or fail to create. Record how many files/
     * tables are created successfully.
     */
    @Override
    public void run() {
      try {
        // This infinity loop will be broken if something crashes or fail to create. This is
        // expected since the master will shutdown at a certain time.
        while (true) {
          if (mOpType == 0) {
            try {
              mTfs.getOutStream(new TachyonURI(TEST_FILE_DIR + mSuccessNum)).close();
            } catch (IOException ioe) {
              break;
            }
          } else if (mOpType == 1) {
            // TODO(gene): Add this back when there is new RawTable client API.
            // if (mTfs.createRawTable(new TachyonURI(TEST_TABLE_DIR + mSuccessNum), 1) == -1) {
            // break;
            // }
          }
          // The create operation may succeed at the master side but still returns false due to the
          // shutdown. So the mSuccessNum may be less than the actual success number.
          mSuccessNum ++;
          CommonUtils.sleepMs(100);
        }
      } catch (Exception e) {
        // Something crashed. Stop the thread.
      }
    }
  }

  private static final int TEST_BLOCK_SIZE = 128;
  private static final String TEST_FILE_DIR = "/files/";
  private static final int TEST_NUM_MASTERS = 3;
  private static final String TEST_TABLE_DIR = "/tables/";
  private static final long TEST_TIME_MS = Constants.SECOND_MS;

  private ClientThread mCreateFileThread = null;
  private ClientThread mCreateTableThread = null;
  /** Executor for running client threads */
  private final ExecutorService mExecutorsForClient = Executors.newFixedThreadPool(2);
  /** Executor for constructing MasterInfo */
  private final ExecutorService mExecutorsForMasterInfo = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  @After
  public final void after() throws Exception {
    mExecutorsForClient.shutdown();
    mExecutorsForMasterInfo.shutdown();
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws Exception {
    System.setProperty("fs.hdfs.impl.disable.cache", "true");
  }

  private FileSystemMaster createFsMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterTachyonConf);
  }

  /**
   * Reproduce the journal and check if the state is correct.
   */
  private void reproduceAndCheckState(int successFiles, int successTables) throws IOException,
      InvalidPathException, FileDoesNotExistException, TableDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    int actualFiles = fsMaster.getFileInfoList(fsMaster.getFileId(
        new TachyonURI(TEST_FILE_DIR))).size();
    Assert.assertTrue((successFiles == actualFiles) || (successFiles + 1 == actualFiles));
    for (int f = 0; f < successFiles; f ++) {
      Assert.assertTrue(fsMaster.getFileId(new TachyonURI(TEST_FILE_DIR + f)) != -1);
    }

    // TODO(gene): Add this back when there is new RawTable client API.
//    int actualTables = fsMaster.getFileInfoList(fsMaster.getFileId(
//        new TachyonURI(TEST_TABLE_DIR))).size();
//    Assert.assertTrue((successTables == actualTables) || (successTables + 1 == actualTables));
//    for (int t = 0; t < successTables; t ++) {
//      Assert.assertTrue(fsMaster.getRawTableId(new TachyonURI(TEST_TABLE_DIR + t)) != -1);
//    }
    fsMaster.stop();
  }

  private LocalTachyonClusterMultiMaster setupMultiMasterCluster() throws IOException {
    // Setup and start the tachyon-ft cluster.
    LocalTachyonClusterMultiMaster cluster = new LocalTachyonClusterMultiMaster(100,
        TEST_NUM_MASTERS, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterTachyonConf = cluster.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(0, cluster.getClient());
    mCreateTableThread = new ClientThread(1, cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    mExecutorsForClient.submit(mCreateTableThread);
    return cluster;
  }

  private LocalTachyonCluster setupSingleMasterCluster() throws IOException {
    // Setup and start the local tachyon cluster.
    LocalTachyonCluster cluster = new LocalTachyonCluster(100, 100, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterTachyonConf = cluster.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(0, cluster.getClient());
    mCreateTableThread = new ClientThread(1, cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    mExecutorsForClient.submit(mCreateTableThread);
    return cluster;
  }

  @Test
  public void singleMasterJournalCrashIntegrationTest() throws Exception {
    LocalTachyonCluster cluster = setupSingleMasterCluster();
    CommonUtils.sleepMs(TEST_TIME_MS);
    // Shutdown the cluster
    cluster.stopTFS();
    CommonUtils.sleepMs(TEST_TIME_MS);
    // Ensure the client threads are stopped.
    mExecutorsForClient.shutdown();
    mExecutorsForClient.awaitTermination(TEST_TIME_MS, TimeUnit.MILLISECONDS);
    reproduceAndCheckState(mCreateFileThread.getSuccessNum(), mCreateTableThread.getSuccessNum());
    // clean up
    cluster.stopUFS();
  }

  @Ignore
  @Test
  public void multiMasterJournalCrashIntegrationTest() throws Exception {
    LocalTachyonClusterMultiMaster cluster = setupMultiMasterCluster();
    // Kill the leader one by one.
    for (int kills = 0; kills < TEST_NUM_MASTERS; kills ++) {
      CommonUtils.sleepMs(TEST_TIME_MS);
      Assert.assertTrue(cluster.killLeader());
    }
    cluster.stopTFS();
    CommonUtils.sleepMs(TEST_TIME_MS);
    // Ensure the client threads are stopped.
    mExecutorsForClient.shutdown();
    while (!mExecutorsForClient.awaitTermination(TEST_TIME_MS, TimeUnit.MILLISECONDS)) {}
    reproduceAndCheckState(mCreateFileThread.getSuccessNum(), mCreateTableThread.getSuccessNum());
    // clean up
    cluster.stopUFS();
  }
}
