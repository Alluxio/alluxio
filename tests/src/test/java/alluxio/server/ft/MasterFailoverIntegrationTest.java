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

package alluxio.server.ft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystem;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class MasterFailoverIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(MasterFailoverIntegrationTest.class);

  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();
  private static final long DELETE_DELAY = 5 * Constants.SECOND_MS;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster;

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  private FileSystem mFileSystem;

  private static final int CLUSTER_WAIT_TIMEOUT_MS = 120 * Constants.SECOND_MS;

  // An under file system which has slow directory deletion.
  private static final UnderFileSystem UFS =
      new DelegatingUnderFileSystem(UnderFileSystem.Factory.create(LOCAL_UFS_PATH,
          Configuration.global())) {
        @Override
        public boolean deleteDirectory(String path) throws IOException {
          CommonUtils.sleepMs(DELETE_DELAY);
          return mUfs.deleteDirectory(path);
        }

        @Override
        public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
          CommonUtils.sleepMs(DELETE_DELAY);
          return mUfs.deleteDirectory(path, options);
        }
      };

  @Rule
  public TestName mTestName = new TestName();

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new DelegatingUnderFileSystemFactory(UFS));

  @Before
  public final void before() throws Exception {
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(2);
    mMultiMasterLocalAlluxioCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "60sec");
    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec");
    Configuration.set(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT, "30sec");
    Configuration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "0sec");
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        DelegatingUnderFileSystemFactory.DELEGATING_SCHEME + "://" + LOCAL_UFS_PATH);
    mMultiMasterLocalAlluxioCluster.start();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
    mMultiMasterLocalAlluxioCluster.stop();
  }

  @Test
  public void failoverJournalFencingTest() throws Exception {
    // This test verifies that when a master fails over due to Zookeeper disconnection, outstanding
    // threads on the master are not allowed to write to the journal.
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystem.createDirectory(dir);
    DeleteThread deleteThread = new DeleteThread(dir);
    deleteThread.start();
    // Give the delete thread a chance to start.
    Thread.sleep(500);
    mMultiMasterLocalAlluxioCluster.stopZk();
    // Give master a chance to notice that ZK is dead and trigger failover.
    Thread.sleep(10000);
    mMultiMasterLocalAlluxioCluster.restartZk();
    deleteThread.join();
    // After failing on the original master, the delete should be retried on the new master.
    assertFalse(mFileSystem.exists(dir));
    // Restart to make sure the journal is consistent (we didn't write two delete entries for /dir).
    mMultiMasterLocalAlluxioCluster.restartMasters();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient(); // need new client after restart
    assertEquals(0, mFileSystem.listStatus(new AlluxioURI("/")).size());
  }

  private class DeleteThread extends Thread {
    public final AlluxioURI mDir;

    public DeleteThread(AlluxioURI dir) {
      mDir = dir;
    }

    @Override
    public void run() {
      try {
        LOG.info("Starting to delete {}", mDir);
        mFileSystem.delete(mDir);
        LOG.info("Deleted {}", mDir);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void failoverAsyncLoadInodeTest() throws Exception {
    /* This test verifies that when a master fails over before those TO_BE_PERSISTED files
     * could be submitted for persisting, the new master should asynchronously submit those
     * files for persisting */
    int numOfFiles = 10;
    String filePrefix = "/file-";
    List<String> fileNames = new ArrayList(numOfFiles);

    for (int i = 0; i < numOfFiles; i++)
    {
      String fileName = filePrefix + Integer.toString(i + 1);
      fileNames.add(fileName);
      AlluxioURI fileToCreate = new AlluxioURI(fileName);
      CreateFilePOptions createOption = CreateFilePOptions.newBuilder()
              .setWriteType(WritePType.ASYNC_THROUGH)
              .setPersistenceWaitTime(1)
              .build();
      FileOutStream fos = mFileSystem.createFile(fileToCreate, createOption);
      fos.close();
      assertTrue(mFileSystem.exists(fileToCreate));
      URIStatus fileStat = mFileSystem.getStatus(fileToCreate);
      assertFalse(fileStat.isPersisted());
    }

    mMultiMasterLocalAlluxioCluster.stopLeader();
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);

    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
    CommonUtils.waitFor("all files persisted", () -> allFilesPersisted(fileNames),
            WaitForOptions.defaults().setTimeoutMs((int) TimeUnit.MINUTES.toMillis(5)));

    for (String fileName : fileNames)
    {
      AlluxioURI fileCreated = new AlluxioURI(fileName);
      URIStatus fileStat = mFileSystem.getStatus(fileCreated);
      assertTrue(fileStat.isPersisted());
    }
  }

  public boolean allFilesPersisted(List<String> files)
  {
    for (String file : files)
    {
      try {
        AlluxioURI fileToCreate = new AlluxioURI(file);
        URIStatus fileStat = mFileSystem.getStatus(fileToCreate);
        if (!fileStat.isPersisted()) {
          return false;
        }
      } catch (IOException | AlluxioException e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }
}
