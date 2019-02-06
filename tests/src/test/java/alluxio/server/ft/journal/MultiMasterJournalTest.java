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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MultiMasterJournalTest {
  private MultiMasterLocalAlluxioCluster mCluster;

  @Before
  public void before() throws Exception {
    mCluster = new MultiMasterLocalAlluxioCluster(2, 2);
    mCluster.initConfiguration();
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 5);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 100);
    mCluster.start();
  }

  @Test
  public void testCheckpointReplay() throws Exception {
    // Trigger a checkpoint.
    for (int i = 0; i < 10; i++) {
      mCluster.getClient().createFile(new AlluxioURI("/" + i)).close();
    }
    UfsJournal journal = new UfsJournal(URIUtils.appendPathOrDie(JournalUtils.getJournalLocation(),
        Constants.FILE_SYSTEM_MASTER_NAME), new NoopMaster(""), 0);
    CommonUtils.waitFor("checkpoint to be written", () -> {
      UfsJournalSnapshot snapshot;
      try {
        snapshot = UfsJournalSnapshot.getSnapshot(journal);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return !snapshot.getCheckpoints().isEmpty();
    });
    mCluster.restartMasters();
    assertEquals("The cluster should remember the 10 files", 10,
        mCluster.getClient().listStatus(new AlluxioURI("/")).size());
  }
}
