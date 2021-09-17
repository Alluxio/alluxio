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

package alluxio.server.ft.journal.ufs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Rule;
import org.junit.Test;

public class SecondaryMasterTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeSecondary(true)
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
          .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 10)
          .setProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 1)
          .build();

  @Test
  public void secondaryShouldCreateCheckpoints() throws Exception {
    FileSystem fs = mClusterResource.get().getClient();
    // Create a bunch of directories to trigger a checkpoint.
    for (int i = 0; i < 10; i++) {
      fs.createDirectory(new AlluxioURI("/dir" + i));
    }
    IntegrationTestUtils.waitForUfsJournalCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME);
  }
}
