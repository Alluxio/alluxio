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
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.master.file.FileSystemMaster;
import tachyon.util.CommonUtils;
import tachyon.util.IdUtils;

/**
 * Test master journal for cluster terminating. Assert that test can replay the editlog and
 * reproduce the correct state. Test both the single master(tachyon) and multi masters(tachyon-ft).
 */
public class JournalShutdownIntegrationTest {

  /**
   * Hold a client and keep creating files.
   */
  class ClientThread implements Runnable {
    /** The number of successfully created files. */
    private int mSuccessNum = 0;

    private final TachyonFileSystem mTfs;

    public ClientThread(TachyonFileSystem tfs) {
      mTfs = tfs;
    }

    public int getSuccessNum() {
      return mSuccessNum;
    }

    /**
     * Keep creating files until something crashes or fails to be created. Record how many files are
     * created successfully.
     */
    @Override
    public void run() {
      try {
        // This infinity loop will be broken if something crashes or fails to be created. This is
        // expected since the master will shutdown at a certain time.
        while (true) {
          try {
            mTfs.getOutStream(new TachyonURI(TEST_FILE_DIR + mSuccessNum)).close();
          } catch (IOException ioe) {
            break;
          }
          // The create operation may succeed at the master side but still return false due to the
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
  private static final long TEST_TIME_MS = Constants.SECOND_MS;

  private ClientThread mCreateFileThread = null;
  /** Executor for running client threads */
  private final ExecutorService mExecutorsForClient = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  @After
  public final void after() throws Exception {
    mExecutorsForClient.shutdown();
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
  private void reproduceAndCheckState(int successFiles) throws IOException,
      InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    int actualFiles =
        fsMaster.getFileInfoList(fsMaster.getFileId(new TachyonURI(TEST_FILE_DIR))).size();
    Assert.assertTrue((successFiles == actualFiles) || (successFiles + 1 == actualFiles));
    for (int f = 0; f < successFiles; f ++) {
      Assert.assertTrue(
          fsMaster.getFileId(new TachyonURI(TEST_FILE_DIR + f)) != IdUtils.INVALID_FILE_ID);
    }

    fsMaster.stop();
  }

  private LocalTachyonClusterMultiMaster setupMultiMasterCluster()
      throws IOException, ConnectionFailedException {
    // Setup and start the tachyon-ft cluster.
    LocalTachyonClusterMultiMaster cluster =
        new LocalTachyonClusterMultiMaster(100, TEST_NUM_MASTERS, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterTachyonConf = cluster.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    return cluster;
  }

  private LocalTachyonCluster setupSingleMasterCluster()
      throws IOException, ConnectionFailedException {
    // Setup and start the local tachyon cluster.
    LocalTachyonCluster cluster = new LocalTachyonCluster(100, 100, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterTachyonConf = cluster.getMasterTachyonConf();
    mCreateFileThread = new ClientThread(cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
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
    reproduceAndCheckState(mCreateFileThread.getSuccessNum());
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
    while (!mExecutorsForClient.awaitTermination(TEST_TIME_MS, TimeUnit.MILLISECONDS)) {
    }
    reproduceAndCheckState(mCreateFileThread.getSuccessNum());
    // clean up
    cluster.stopUFS();
  }
}
