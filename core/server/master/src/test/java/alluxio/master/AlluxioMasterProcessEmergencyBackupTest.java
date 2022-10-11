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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.NodeState;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalLogWriter;
import alluxio.master.journal.ufs.UfsJournalSingleMasterPrimarySelector;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.Collections;
import java.util.NoSuchElementException;

/**
 * This tests the emergency backup feature when encountering journal corruption.
 */
public class AlluxioMasterProcessEmergencyBackupTest {
  @Rule
  public PortReservationRule mRpcPortRule = new PortReservationRule();
  @Rule
  public PortReservationRule mWebPortRule = new PortReservationRule();
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.MASTER_RPC_PORT, mRpcPortRule.getPort());
    Configuration.set(PropertyKey.MASTER_WEB_PORT, mWebPortRule.getPort());
    Configuration.set(PropertyKey.MASTER_METASTORE_DIR, mFolder.newFolder("metastore"));
    Configuration.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mFolder.newFolder("journal"));
  }

  @Test
  public void failToGainPrimacyWhenJournalCorrupted() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED, false);
    URI journalLocation = JournalUtils.getJournalLocation();
    JournalSystem journalSystem = new JournalSystem.Builder()
        .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER);
    AlluxioMasterProcess masterProcess = new AlluxioMasterProcess(journalSystem,
        new UfsJournalSingleMasterPrimarySelector());
    corruptJournalAndStartMasterProcess(masterProcess, journalLocation);
  }

  @Test
  public void failToGainPrimacyWhenJournalCorruptedHA() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED, false);
    URI journalLocation = JournalUtils.getJournalLocation();
    JournalSystem journalSystem = new JournalSystem.Builder()
        .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER);
    ControllablePrimarySelector primarySelector = new ControllablePrimarySelector();
    AlluxioMasterProcess masterProcess =
        new AlluxioMasterProcess(journalSystem, primarySelector);
    primarySelector.setState(NodeState.PRIMARY);
    corruptJournalAndStartMasterProcess(masterProcess, journalLocation);
  }

  private void corruptJournalAndStartMasterProcess(AlluxioMasterProcess masterProcess,
      URI journalLocation) throws Exception {
    assertTrue(masterProcess.mJournalSystem instanceof UfsJournalSystem);
    masterProcess.mJournalSystem.format();
    // corrupt the journal
    UfsJournal fsMaster =
        new UfsJournal(URIUtils.appendPathOrDie(journalLocation, "FileSystemMaster"),
            new NoopMaster(), 0, Collections::emptySet);
    fsMaster.start();
    fsMaster.gainPrimacy();
    long nextSN = 0;
    try (UfsJournalLogWriter writer = new UfsJournalLogWriter(fsMaster, nextSN)) {
      Journal.JournalEntry entry = Journal.JournalEntry.newBuilder()
          .setSequenceNumber(nextSN)
          .setDeleteFile(File.DeleteFileEntry.newBuilder()
              .setId(4563728) // random non-zero ID number (zero would delete the root)
              .setPath("/nonexistant")
              .build())
          .build();
      writer.write(entry);
      writer.flush();
    }
    // comes from mJournalSystem#gainPrimacy
    RuntimeException exception = assertThrows(RuntimeException.class, masterProcess::start);
    assertTrue(exception.getMessage().contains(NoSuchElementException.class.getName()));
    // if AlluxioMasterProcess#start throws an exception, then #stop will get called
    masterProcess.stop();
    assertTrue(masterProcess.isStopped());
  }
}
