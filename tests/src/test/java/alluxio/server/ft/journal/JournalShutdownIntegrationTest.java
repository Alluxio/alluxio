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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.SystemPropertyRule;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.master.FsMasterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystem;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test master journal for cluster terminating. Assert that test can replay the log and reproduce
 * the correct state. Test both the single master and multi masters.
 */
@Ignore
public class JournalShutdownIntegrationTest extends BaseIntegrationTest {
  @ClassRule
  public static SystemPropertyRule sDisableHdfsCacheRule =
      new SystemPropertyRule("fs.hdfs.impl.disable.cache", "true");

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test",
      ServerConfiguration.global());

  @Rule
  private TestName mTestName = new TestName();

  @Rule
  public ConfigurationRule mConfigRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "100")
          .put(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2")
          .put(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "128")
          .put(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec").build(),
          ServerConfiguration.global());

  private static final long SHUTDOWN_TIME_MS = 15 * Constants.SECOND_MS;
  private static final String TEST_FILE_DIR = "/files/";
  private static final int TEST_NUM_MASTERS = 3;
  private static final long TEST_TIME_MS = Constants.SECOND_MS;

  private ClientThread mCreateFileThread;
  /** Executor for running client threads. */
  private ExecutorService mExecutorsForClient;
  private FileSystemContext mFsContext;

  @Before
  public final void before() throws Exception {
    mExecutorsForClient = Executors.newFixedThreadPool(1);
    mFsContext = FileSystemContext.create(ServerConfiguration.global());
  }

  @After
  public final void after() throws Exception {
    mExecutorsForClient.shutdown();
    mFsContext.close();
    ServerConfiguration.reset();
    ServerConfiguration.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
  }

  @Test
  public void singleMasterJournalStopIntegration() throws Exception {
    MultiProcessCluster cluster =
        MultiProcessCluster.newBuilder(PortCoordination.JOURNAL_STOP_SINGLE_MASTER)
            .setClusterName("singleMasterJournalStopIntegration")
            .setNumWorkers(0)
            .setNumMasters(1)
            .build();
    try {
      cluster.start();
      FileSystem fs = cluster.getFileSystemClient();
      runCreateFileThread(fs);
      cluster.waitForAndKillPrimaryMaster(10 * Constants.SECOND_MS);
      awaitClientTermination();
      cluster.startMaster(0);
      int actualFiles = fs.listStatus(new AlluxioURI(TEST_FILE_DIR)).size();
      int successFiles = mCreateFileThread.getSuccessNum();
      assertTrue(
          String.format("successFiles: %s, actualFiles: %s", successFiles, actualFiles),
          (successFiles == actualFiles) || (successFiles + 1 == actualFiles));
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  /*
   * We use the external cluster for this test due to flakiness issues when running in a single JVM.
   */
  @Test
  public void multiMasterJournalStopIntegration() throws Exception {
    MultiProcessCluster cluster =
        MultiProcessCluster.newBuilder(PortCoordination.JOURNAL_STOP_MULTI_MASTER)
            .setClusterName("multiMasterJournalStopIntegration")
            .setNumWorkers(0)
            .setNumMasters(TEST_NUM_MASTERS)
            // Cannot go lower than 2x the tick time. Curator testing cluster tick time is 3s and
            // cannot be overridden until later versions of Curator.
            .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "6s")
            .build();
    try {
      cluster.start();
      FileSystem fs = cluster.getFileSystemClient();
      runCreateFileThread(fs);
      for (int i = 0; i < TEST_NUM_MASTERS; i++) {
        cluster.waitForAndKillPrimaryMaster(30 * Constants.SECOND_MS);
      }
      awaitClientTermination();
      cluster.startMaster(0);
      int actualFiles = fs.listStatus(new AlluxioURI(TEST_FILE_DIR)).size();
      int successFiles = mCreateFileThread.getSuccessNum();
      assertTrue(
          String.format("successFiles: %s, actualFiles: %s", successFiles, actualFiles),
          (successFiles == actualFiles) || (successFiles + 1 == actualFiles));
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void singleMasterMountUnmountJournal() throws Exception {
    LocalAlluxioCluster cluster = setupSingleMasterCluster();
    UnderFileSystemFactory factory = mountUnmount(cluster.getClient());
    // Shutdown the cluster
    cluster.stopFS();
    CommonUtils.sleepMs(TEST_TIME_MS);
    awaitClientTermination();
    // Fail the creation of UFS
    doThrow(new RuntimeException()).when(factory).create(anyString(),
        any(UnderFileSystemConfiguration.class));
    createFsMasterFromJournal().close();
  }

  @Test
  public void multiMasterMountUnmountJournal() throws Exception {
    MultiMasterLocalAlluxioCluster cluster = null;
    UnderFileSystemFactory factory = null;
    try {
      cluster = new MultiMasterLocalAlluxioCluster(TEST_NUM_MASTERS);
      cluster.initConfiguration(
          IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
      cluster.start();
      cluster.stopLeader();
      factory = mountUnmount(cluster.getClient());
      // Kill the leader one by one.
      for (int kills = 0; kills < TEST_NUM_MASTERS; kills++) {
        cluster.waitForNewMaster(120 * Constants.SECOND_MS);
        assertTrue(cluster.stopLeader());
      }
    } finally {
      // Shutdown the cluster
      if (cluster != null) {
        cluster.stopFS();
      }
    }
    CommonUtils.sleepMs(TEST_TIME_MS);
    awaitClientTermination();
    // Fail the creation of UFS
    doThrow(new RuntimeException()).when(factory).create(anyString(),
        any(UnderFileSystemConfiguration.class));
    createFsMasterFromJournal().close();
  }

  /**
   * @param fs Filesystem client
   * @return a spied UFS factory mounted to and then unmounted from fs
   */
  private UnderFileSystemFactory mountUnmount(FileSystem fs) throws Exception {
    SleepingUnderFileSystem sleepingUfs = new SleepingUnderFileSystem(new AlluxioURI("sleep:///"),
        new SleepingUnderFileSystemOptions(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    SleepingUnderFileSystemFactory sleepingUfsFactory =
        new SleepingUnderFileSystemFactory(sleepingUfs);
    UnderFileSystemFactoryRegistry.register(sleepingUfsFactory);
    fs.mount(new AlluxioURI("/mnt"), new AlluxioURI("sleep:///"));
    fs.unmount(new AlluxioURI("/mnt"));
    return spy(sleepingUfsFactory);
  }

  /**
   * Waits for the client to terminate.
   */
  private void awaitClientTermination() throws Exception {
    // Ensure the client threads are stopped.
    mExecutorsForClient.shutdownNow();
    if (!mExecutorsForClient.awaitTermination(SHUTDOWN_TIME_MS, TimeUnit.MILLISECONDS)) {
      throw new Exception("Client thread did not terminate");
    }
  }

  /**
   * Creates file system master from journal.
   */
  private FsMasterResource createFsMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournal();
  }

  /**
   * Sets up and starts a single master cluster.
   */
  private LocalAlluxioCluster setupSingleMasterCluster() throws Exception {
    // Setup and start the local alluxio cluster.
    LocalAlluxioCluster cluster = new LocalAlluxioCluster();
    cluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
    cluster.start();
    return cluster;
  }

  /**
   * Starts a file-creating thread and runs it for some time, at least until it has created one
   * file.
   *
   * @param fs a file system client to use for creating files
   */
  private void runCreateFileThread(FileSystem fs) {
    mCreateFileThread = new ClientThread(0, fs);
    mExecutorsForClient.submit(mCreateFileThread);
    CommonUtils.sleepMs(TEST_TIME_MS);
    while (mCreateFileThread.getSuccessNum() == 0) {
      CommonUtils.sleepMs(TEST_TIME_MS);
    }
  }

  /**
   * Hold a client and keep creating files.
   */
  class ClientThread implements Runnable {
    /** The number of successfully created files. */
    private int mSuccessNum = 0;

    private final int mOpType; // 0: create file
    private final FileSystem mFileSystem;

    /**
     * Constructs the client thread.
     *
     * @param opType the create operation type
     * @param fs a file system client to use for creating files
     */
    public ClientThread(int opType, FileSystem fs) {
      mOpType = opType;
      mFileSystem = fs;
    }

    /**
     * Gets the number of files which are successfully created.
     *
     * @return the number of files successfully created
     */
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
        while (!Thread.interrupted()) {
          if (mOpType == 0) {
            try {
              mFileSystem.createFile(new AlluxioURI(TEST_FILE_DIR + mSuccessNum)).close();
            } catch (IOException e) {
              break;
            }
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
}
