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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class MultiMasterJournalTest extends BaseIntegrationTest {
  private MultiMasterLocalAlluxioCluster mCluster;

  @Rule
  public TtlIntervalRule mTtlRule = new TtlIntervalRule(200);

  @Rule
  public TestName mTestName = new TestName();

  @Before
  public void before() throws Exception {
    mCluster = new MultiMasterLocalAlluxioCluster(2, 0);
    mCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 5);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 100);
    mCluster.start();
  }

  @After
  public void after() throws Exception {
    mCluster.stop();
  }

  @Test
  public void testCheckpointReplay() throws Exception {
    triggerAndWaitForCheckpoint();
    mCluster.restartMasters();
    assertEquals("The cluster should remember the 10 files", 10,
        mCluster.getClient().listStatus(new AlluxioURI("/")).size());
  }

  @Test
  public void testTtl() throws Exception {
    // Test that ttls are still applied after restart.
    AlluxioURI file = new AlluxioURI("/file");
    mCluster.getClient().createFile(file).close();
    int ttl = 5 * Constants.SECOND_MS;
    mCluster.getClient().setAttribute(file, SetAttributePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(ttl)
            .setTtlAction(TtlAction.DELETE))
        .build());
    triggerAndWaitForCheckpoint();
    mCluster.restartMasters();
    FileSystem client = mCluster.getClient();
    CommonUtils.waitFor("file to be deleted", () -> {
      try {
        return !client.exists(file);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
  }

  private void triggerAndWaitForCheckpoint() throws Exception {
    FileSystem client = mCluster.getClient();
    for (int i = 0; i < 10; i++) {
      client.createFile(new AlluxioURI("/triggerCheckpoint" + i)).close();
    }
    IntegrationTestUtils.waitForUfsJournalCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME);
  }
}
