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
import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test master journal for cluster terminating. Assert that test can replay the editlog and
 * reproduce the correct state. Test both the single master(alluxio) and multi masters(alluxio-ft).
 */
public class JournalShutdownIntegrationTest {

  /**
   * Hold a client and keep creating files.
   */
  class ClientThread implements Runnable {
    /** The number of successfully created files. */
    private int mSuccessNum = 0;

    private final int mOpType; // 0: create file
    private final FileSystem mFileSystem;

    public ClientThread(int opType, FileSystem fs) {
      mOpType = opType;
      mFileSystem = fs;
    }

    public int getSuccessNum() {
      return mSuccessNum;
    }

    /**
     * Keep creating files until something crashes or fail to create. Record how many files are
     * created successfully.
     */
    @Override
    public void run() {
      try {
        // This infinity loop will be broken if something crashes or fail to create. This is
        // expected since the master will shutdown at a certain time.
        while (true) {
          if (mOpType == 0) {
            try {
              mFileSystem.createFile(new AlluxioURI(TEST_FILE_DIR + mSuccessNum)).close();
            } catch (IOException e) {
              break;
            }
          } else if (mOpType == 1) {
            // TODO(gene): Add this back when there is new RawTable client API.
            // if (mFileSystem.createRawTable(new AlluxioURI(TEST_TABLE_DIR + mSuccessNum), 1) ==
            // -1) {
            // break;
            // }
          }
          // The create operation may succeed at the master side but still returns false due to the
          // shutdown. So the mSuccessNum may be less than the actual success number.
          mSuccessNum++;
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
  /** Executor for running client threads. */
  private final ExecutorService mExecutorsForClient = Executors.newFixedThreadPool(1);
  private Configuration mMasterConfiguration = null;

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
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterConfiguration);
  }

  /**
   * Reproduce the journal and check if the state is correct.
   */
  private void reproduceAndCheckState(int successFiles) throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    int actualFiles = fsMaster.getFileInfoList(new AlluxioURI(TEST_FILE_DIR)).size();
    Assert.assertTrue((successFiles == actualFiles) || (successFiles + 1 == actualFiles));
    for (int f = 0; f < successFiles; f++) {
      Assert.assertTrue(
          fsMaster.getFileId(new AlluxioURI(TEST_FILE_DIR + f)) != IdUtils.INVALID_FILE_ID);
    }
    fsMaster.stop();
  }

  private MultiMasterLocalAlluxioCluster setupMultiMasterCluster()
      throws IOException, ConnectionFailedException {
    // Setup and start the alluxio-ft cluster.
    MultiMasterLocalAlluxioCluster cluster =
        new MultiMasterLocalAlluxioCluster(100, TEST_NUM_MASTERS, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterConfiguration = cluster.getMasterConf();
    mCreateFileThread = new ClientThread(0, cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    return cluster;
  }

  private LocalAlluxioCluster setupSingleMasterCluster()
      throws IOException, ConnectionFailedException {
    // Setup and start the local alluxio cluster.
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(100, TEST_BLOCK_SIZE);
    cluster.start();
    mMasterConfiguration = cluster.getMasterConf();
    mCreateFileThread = new ClientThread(0, cluster.getClient());
    mExecutorsForClient.submit(mCreateFileThread);
    return cluster;
  }

  @Test
  public void singleMasterJournalCrashIntegrationTest() throws Exception {
    LocalAlluxioCluster cluster = setupSingleMasterCluster();
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
    MultiMasterLocalAlluxioCluster cluster = setupMultiMasterCluster();
    // Kill the leader one by one.
    for (int kills = 0; kills < TEST_NUM_MASTERS; kills++) {
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
