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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystem;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.CommonUtils;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class MasterFailoverIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(MasterFailoverIntegrationTest.class);

  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();
  private static final long DELETE_DELAY = 5 * Constants.SECOND_MS;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster;
  private FileSystem mFileSystem;

  // An under file system which has slow directory deletion.
  private static final UnderFileSystem UFS =
      new DelegatingUnderFileSystem(UnderFileSystem.Factory.create(LOCAL_UFS_PATH,
          ServerConfiguration.global())) {
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
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "60sec");
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec");
    ServerConfiguration.set(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT, "30sec");
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "0sec");
    ServerConfiguration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        DelegatingUnderFileSystemFactory.DELEGATING_SCHEME + "://" + LOCAL_UFS_PATH);
    mMultiMasterLocalAlluxioCluster.start();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();
  }

  @Ignore
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
    Thread.sleep(5000);
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
}
