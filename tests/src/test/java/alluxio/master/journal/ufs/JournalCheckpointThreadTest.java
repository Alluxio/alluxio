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

package alluxio.master.journal.ufs;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.master.MockMaster;
import alluxio.master.journal.JournalCheckpointThread;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalWriterOptions;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

/**
 * Unit tests for {@link alluxio.master.journal.JournalCheckpointThread}.
 */
public final class JournalCheckpointThreadTest extends BaseIntegrationTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;
  private UnderFileSystem mUfs;

  @Before
  public void before() throws Exception {
    URI location = URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster");
    mUfs = Mockito.spy(UnderFileSystem.Factory.create(location));
    mJournal = new UfsJournal(location, mUfs);
  }

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * The checkpoint thread replays all the logs and checkpoints periodically if not shutdown.
   */
  @Test
  public void checkpointBeforeShutdown() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2");
    buildCompletedLog(0, 10);
    buildIncompleteLog(10, 15);
    MockMaster mockMaster = new MockMaster();
    JournalCheckpointThread checkpointThread = new JournalCheckpointThread(mockMaster, mJournal);
    checkpointThread.start();
    CommonUtils.waitFor("checkpoint", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
          if (!snapshot.getCheckpoints().isEmpty()
              && snapshot.getCheckpoints().get(snapshot.getCheckpoints().size() - 1).getEnd()
              == 10) {
            return true;
          }
        } catch (IOException e) {
          return false;
        }
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(20000));
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(1, snapshot.getCheckpoints().size());
    Assert.assertEquals(10, snapshot.getCheckpoints().get(0).getEnd());
    checkpointThread.awaitTermination();
  }

  /**
   * The checkpoint thread replays all the logs before shutting down.
   */
  @Test
  public void checkpointAfterShutdown() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2");
    buildCompletedLog(0, 10);
    buildIncompleteLog(10, 15);
    MockMaster mockMaster = new MockMaster();
    JournalCheckpointThread checkpointThread = new JournalCheckpointThread(mockMaster, mJournal);
    checkpointThread.start();
    checkpointThread.awaitTermination();

    // Make sure all the journal entries have been processed. Note that it is not necessary that
    // the they are checkpointed.
    Iterator<Journal.JournalEntry> it = mockMaster.getJournalEntryIterator();
    int sz = 0;
    while (it.hasNext()) {
      it.next();
      sz++;
    }
    Assert.assertEquals(10, sz);
  }

  /**
   * Builds complete log.
   */
  private void buildCompletedLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(start));
    for (long i = start; i < end; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();
  }

  /**
   * Builds incomplete log.
   */
  private void buildIncompleteLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    buildCompletedLog(start, end);
    Assert.assertTrue(
        mUfs.renameFile(UfsJournalFile.encodeLogFileLocation(mJournal, start, end).toString(),
            UfsJournalFile
                .encodeLogFileLocation(mJournal, start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
                .toString()));
  }

  /**
   * Creates a dummy journal entry with the given sequence number.
   *
   * @param sequenceNumber the sequence number
   * @return the journal entry
   */
  private Journal.JournalEntry newEntry(long sequenceNumber) {
    return Journal.JournalEntry.newBuilder().setSequenceNumber(sequenceNumber).build();
  }
}
