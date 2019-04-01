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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;

import org.junit.Before;
import org.junit.Test;

public class MultiMasterJournalTest extends BaseIntegrationTest {
  private MultiMasterLocalAlluxioCluster mCluster;

  @Before
  public void before() throws Exception {
    mCluster = new MultiMasterLocalAlluxioCluster(2, 0);
    mCluster.initConfiguration();
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 5);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 100);
    mCluster.start();
  }

  @Test
  public void testCheckpointReplay() throws Exception {
    // Trigger a checkpoint.
    for (int i = 0; i < 10; i++) {
      mCluster.getClient().createFile(new AlluxioURI("/" + i)).close();
    }
    IntegrationTestUtils.waitForCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME);
    mCluster.restartMasters();
    assertEquals("The cluster should remember the 10 files", 10,
        mCluster.getClient().listStatus(new AlluxioURI("/")).size());
  }
}
