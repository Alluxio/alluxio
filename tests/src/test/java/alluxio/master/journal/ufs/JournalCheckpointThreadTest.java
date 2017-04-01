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
import alluxio.PropertyKey;
import alluxio.master.Master;
import alluxio.master.journal.JournalCheckpointThread;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalWriterCreateOptions;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;
import org.apache.thrift.TProcessor;
import org.eclipse.jetty.util.ArrayQueue;
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
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Unit tests for {@link alluxio.master.journal.JournalCheckpointThread}.
 */
public final class JournalCheckpointThreadTest {
  private static class FakeMaster implements Master {
    private Queue<Journal.JournalEntry> mEntries;

    FakeMaster() {
      mEntries = new ArrayQueue<>();
    }

    @Override
    public Map<String, TProcessor> getServices() {
      return null;
    }

    @Override
    public String getName() {
      return "FakeMaster";
    }

    @Override
    public Set<Class<?>> getDependencies() {
      return null;
    }

    @Override
    public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
      mEntries.add(entry);
    }

    @Override
    public void start(boolean isPrimary) throws IOException {
    }

    @Override
    public void stop() throws IOException {
    }

    @Override
    public Iterator<Journal.JournalEntry> iterator() {
      return mEntries.iterator();
    }
  }

  private static final long CHECKPOINT_SIZE = 10;
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;
  private UnderFileSystem mUfs;

  @Before
  public void before() throws Exception {
    URI location = URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster");
    mUfs = Mockito.spy(UnderFileSystem.Factory.get(location.toString()));
    mJournal = new UfsJournal(location, mUfs);
  }

  @After
  public void after() throws Exception {
    Configuration.defaultInit();
  }

  @Test
  public void checkpointBeforeShutdown() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2");
    buildCompletedLog(0, 10);
    buildIncompleteLog(10, 15);
    FakeMaster fakeMaster = new FakeMaster();
    JournalCheckpointThread checkpointThread = new JournalCheckpointThread(fakeMaster, mJournal);
    checkpointThread.start();
    CommonUtils.waitFor("checkpoint", new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void input) {
        try {
          UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
          if (!snapshot.mCheckpoints.isEmpty()
              && snapshot.mCheckpoints.get(snapshot.mCheckpoints.size() - 1).getEnd() == 10) {
            return true;
          }
        } catch (IOException e) {
          return false;
        }
        return false;
      }
    }, WaitForOptions.defaults().setTimeout(20000));
    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertEquals(5, snapshot.mCheckpoints.size());
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals(i * 2 + 2, snapshot.mCheckpoints.get(i).getEnd());
    }
    checkpointThread.shutdownAndJoin();
  }

  private void buildCompletedLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    JournalWriter writer = mJournal.getWriter(
        JournalWriterCreateOptions.defaults().setPrimary(true).setNextSequenceNumber(start));
    for (long i = start; i < end; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();
  }

  private void buildIncompleteLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    buildCompletedLog(start, end);
    Assert.assertTrue(mUfs.renameFile(mJournal.encodeLogFileLocation(start, end).toString(),
        mJournal.encodeLogFileLocation(start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString()));
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
