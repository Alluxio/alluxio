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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.BaseIntegrationTest;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.SystemPropertyRule;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.sleepfs.SleepingUnderFileSystem;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemFactory;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemOptions;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test master journal for cluster terminating. Assert that test can replay the log and reproduce
 * the correct state. Test both the single master and multi masters.
 */
public class JournalShutdownIntegrationTest extends BaseIntegrationTest {
  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test");

  private static final long SHUTDOWN_TIME_MS = 15 * Constants.SECOND_MS;
  private static final String TEST_FILE_DIR = "/files/";
  private static final int TEST_NUM_MASTERS = 3;
  private static final long TEST_TIME_MS = Constants.SECOND_MS;

  private ClientThread mCreateFileThread;
  /** Executor for running client threads. */
  private ExecutorService mExecutorsForClient;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "100")
          .put(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2")
          .put(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "32").build());

  @ClassRule
  public static SystemPropertyRule sDisableHdfsCacheRule =
      new SystemPropertyRule("fs.hdfs.impl.disable.cache", "true");

  @After
  public final void after() throws Exception {
    mExecutorsForClient.shutdown();
    ConfigurationTestUtils.resetConfiguration();
    FileSystemContext.INSTANCE.reset();
  }

  @Before
  public final void before() throws Exception {
    mExecutorsForClient = Executors.newFixedThreadPool(1);
  }

  @Test
  public void singleMasterJournalStopIntegration() throws Exception {
    LocalAlluxioCluster cluster = setupSingleMasterCluster();
    runCreateFileThread(cluster.getClient());
    System.out.println(Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER));
    // Shutdown the cluster
    cluster.stopFS();
    CommonUtils.sleepMs(TEST_TIME_MS);
    awaitClientTermination();
    reproduceAndCheckState(mCreateFileThread.getSuccessNum());
  }

  @Test
  public void multiMasterJournalStopIntegration() throws Exception {
    MultiMasterLocalAlluxioCluster cluster = setupMultiMasterCluster();
    runCreateFileThread(cluster.getClient());
    // Kill the leader one by one.
    for (int kills = 0; kills < TEST_NUM_MASTERS; kills++) {
      cluster.waitForNewMaster(120 * Constants.SECOND_MS);
      Assert.assertTrue(cluster.stopLeader());
    }
    cluster.stopFS();
    awaitClientTermination();
    reproduceAndCheckState(mCreateFileThread.getSuccessNum());
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
    Mockito.doThrow(new RuntimeException()).when(factory).create(Mockito.anyString(),
        Mockito.any(UnderFileSystemConfiguration.class));
    createFsMasterFromJournal();
  }

  @Test
  public void multiMasterMountUnmountJournal() throws Exception {
    MultiMasterLocalAlluxioCluster cluster = setupMultiMasterCluster();
    UnderFileSystemFactory factory = mountUnmount(cluster.getClient());
    // Kill the leader one by one.
    for (int kills = 0; kills < TEST_NUM_MASTERS; kills++) {
      cluster.waitForNewMaster(120 * Constants.SECOND_MS);
      Assert.assertTrue(cluster.stopLeader());
    }
    // Shutdown the cluster
    cluster.stopFS();
    CommonUtils.sleepMs(TEST_TIME_MS);
    awaitClientTermination();
    // Fail the creation of UFS
    Mockito.doThrow(new RuntimeException()).when(factory).create(Mockito.anyString(),
        Mockito.any(UnderFileSystemConfiguration.class));
    createFsMasterFromJournal();
  }

  /**
   * @param fs Filesystem client
   * @return a spied UFS factory mounted to and then unmounted from fs
   */
  private UnderFileSystemFactory mountUnmount(FileSystem fs) throws Exception {
    SleepingUnderFileSystem sleepingUfs = new SleepingUnderFileSystem(new AlluxioURI("sleep:///"),
        new SleepingUnderFileSystemOptions(), UnderFileSystemConfiguration.defaults());
    SleepingUnderFileSystemFactory sleepingUfsFactory =
        new SleepingUnderFileSystemFactory(sleepingUfs);
    UnderFileSystemFactoryRegistry.register(sleepingUfsFactory);
    fs.mount(new AlluxioURI("/mnt"), new AlluxioURI("sleep:///"));
    fs.unmount(new AlluxioURI("/mnt"));
    return Mockito.spy(sleepingUfsFactory);
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
  private MasterRegistry createFsMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournal();
  }

  /**
   * Reproduce the journal and check if the state is correct.
   */
  private void reproduceAndCheckState(int successFiles) throws Exception {
    Assert.assertNotEquals(successFiles, 0);
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    int actualFiles =
        fsMaster.listStatus(new AlluxioURI(TEST_FILE_DIR), ListStatusOptions.defaults()).size();
    Assert.assertTrue((successFiles == actualFiles) || (successFiles + 1 == actualFiles));
    for (int f = 0; f < successFiles; f++) {
      Assert.assertTrue(
          fsMaster.getFileId(new AlluxioURI(TEST_FILE_DIR + f)) != IdUtils.INVALID_FILE_ID);
    }
    registry.stop();
  }

  /**
   * Sets up and starts a multi-master cluster.
   */
  private MultiMasterLocalAlluxioCluster setupMultiMasterCluster() throws Exception {
    // Setup and start the alluxio-ft cluster.
    MultiMasterLocalAlluxioCluster cluster = new MultiMasterLocalAlluxioCluster(TEST_NUM_MASTERS);
    cluster.initConfiguration();
    cluster.start();
    return cluster;
  }

  /**
   * Sets up and starts a single master cluster.
   */
  private LocalAlluxioCluster setupSingleMasterCluster() throws Exception {
    // Setup and start the local alluxio cluster.
    LocalAlluxioCluster cluster = new LocalAlluxioCluster();
    cluster.initConfiguration();
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
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
}
