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

package alluxio.master.journal;

import static alluxio.master.journal.JournalTestUtils.createEmbeddedJournalTestPorts;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.StateLockOptions;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemJournalEntryMerger;
import alluxio.master.file.InodeSyncStream;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class JournalContextTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {JournalType.UFS.name()},
        {JournalType.EMBEDDED.name()}
    });
  }

  private final String mJournalType;

  private JournalSystem mJournalSystem;
  private CoreMasterContext mMasterContext;
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  public JournalContextTest(String journalType) {
    mJournalType = journalType;
  }

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, mJournalType);
    createEmbeddedJournalTestPorts(1);

    mRegistry = new MasterRegistry();
    mJournalSystem = JournalTestUtils.createJournalSystem(mTemporaryFolder);
    mJournalSystem.format();
    mMasterContext = MasterTestUtils.testMasterContext(mJournalSystem);
    new MetricsMasterFactory().create(mRegistry, mMasterContext);
    mBlockMaster = new BlockMasterFactory().create(mRegistry, mMasterContext);

    // start
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
    Configuration.reloadProperties();
  }

  @Test
  public void journalContextBlocksPausing() throws Exception {
    JournalContext journalContext = mBlockMaster.createJournalContext();

    AtomicBoolean paused = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      // the pause lock should block
      try {
        mMasterContext.getStateLockManager().lockExclusive(StateLockOptions.defaults());
        paused.set(true);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to grab state-lock exclusively", e);
      }
    });
    thread.start();

    try {
      // since the journal context is still open, the pause should be blocked
      CommonUtils.sleepMs(100);
      assertFalse(paused.get());

      // after closing the journal context, the pause lock should succeed
      journalContext.close();
      CommonUtils.waitFor("pause lock to succeed", paused::get,
          WaitForOptions.defaults().setTimeoutMs(5 * Constants.SECOND_MS).setInterval(10));
    } finally {
      thread.interrupt();
      thread.join();
    }
  }

  @Test
  public void pauseBlocksJournalContext() throws Exception {
    LockResource lock =
        mMasterContext.getStateLockManager().lockExclusive(StateLockOptions.defaults());

    AtomicBoolean journalContextCreated = new AtomicBoolean(false);
    Runnable run = () -> {
      // new journal contexts should be blocked
      try (JournalContext journalContext = mBlockMaster.createJournalContext()) {
        journalContextCreated.set(true);
      } catch (UnavailableException e) {
        throw new RuntimeException("Failed to create journal context", e);
      }
    };

    Thread thread = new Thread(run);
    Thread thread2 = new Thread(run);
    thread.start();

    try {
      // since state is paused, new contexts should not be created
      CommonUtils.sleepMs(100);
      assertFalse(journalContextCreated.get());

      // after un-pausing, new journal contexts can be created
      lock.close();
      thread2.start();
      CommonUtils.waitFor("journal context created", journalContextCreated::get,
          WaitForOptions.defaults().setTimeoutMs(5 * Constants.SECOND_MS).setInterval(10));
    } finally {
      thread.interrupt();
      thread.join();
      thread2.interrupt();
      thread2.join();
    }
  }

  // See https://github.com/Alluxio/alluxio/issues/13904
  @Test
  public void journalClosedTest() throws Exception {
    // Secondary journals will be closed for operation.
    mJournalSystem.losePrimacy();
    // Validate that createJournalContext fails for standby journals.
    try {
      mBlockMaster.createJournalContext();
      fail("journal context creation should fail in standby journal.");
    } catch (UnavailableException e) {
      // expected.
    }
    // Validate that we haven't leaked state lock while creating journal context.
    assertEquals(0, mMasterContext.getStateLockManager().getSharedWaitersAndHolders().size());
  }

  @Test
  public void stateChangeFairness() throws Exception {
    JournalContext journalContext = mBlockMaster.createJournalContext();

    AtomicBoolean paused = new AtomicBoolean(false);

    ExecutorService service =
        ExecutorServiceFactories.cachedThreadPool("stateChangeFairness").create();

    // create tasks that continually create journal contexts
    for (int i = 0; i < 100; i++) {
      service.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            mBlockMaster.createJournalContext().close();
          } catch (UnavailableException e) {
            // ignore
          }
        }
      });
    }

    // task that attempts to pause the state
    service.submit(() -> {
      // the pause lock should block
      try (LockResource lr =
          mMasterContext.getStateLockManager().lockExclusive(StateLockOptions.defaults())) {
        paused.set(true);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to acquire state-lock exclusively.");
      }
    });

    try {
      // since the journal context is still open, the pause should be blocked
      CommonUtils.sleepMs(100);
      assertFalse(paused.get());

      // after closing the journal context, the pause lock should succeed, even when there are many
      // threads creating journal contexts.
      journalContext.close();
      CommonUtils.waitFor("pause lock to succeed", paused::get,
          WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS).setInterval(10));
    } finally {
      service.shutdownNow();
      service.awaitTermination(5, TimeUnit.SECONDS);
    }

    assertTrue(paused.get());
  }

  @Test
  public void mergeJournal() throws Exception {
    JournalContext journalContext = Mockito.mock(JournalContext.class);
    AlluxioURI uri = new AlluxioURI("/dir/test1");
    List<Journal.JournalEntry> entries = new ArrayList<>();
    doAnswer(invocationOnMock -> {
      entries.add(invocationOnMock.getArgument(0));
      return null;
    }).when(journalContext).append(any(Journal.JournalEntry.class));

    JournalContext mergeContext = new MergeJournalContext(journalContext,
        uri, InodeSyncStream::mergeCreateComplete);
    mergeContext.append(Journal.JournalEntry.newBuilder().setInodeFile(
        File.InodeFileEntry.newBuilder().setId(
            BlockId.createBlockId(1, BlockId.getMaxSequenceNumber())).setLength(2)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test1").setPath(uri.getPath()).build()).build());
    mergeContext.append(Journal.JournalEntry.newBuilder().setInodeFile(
        File.InodeFileEntry.newBuilder().setId(
            BlockId.createBlockId(2, BlockId.getMaxSequenceNumber())).setLength(3)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test2").build()).build());
    mergeContext.append(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(
            BlockId.createBlockId(3, BlockId.getMaxSequenceNumber()))
            .setName("test3_unchanged").build()).build());
    mergeContext.append(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(
            BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()))
            .setName("test2_updated").build()).build());
    mergeContext.append(Journal.JournalEntry.newBuilder().setUpdateInodeFile(
        File.UpdateInodeFileEntry.newBuilder().setId(
            BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()))
            .setLength(200).build()).build());
    mergeContext.close();

    // Merge context only merges the matching file and ignores other entries
    assertEquals(4, entries.size());
    Journal.JournalEntry entry = entries.get(0);
    assertNotNull(entry.getInodeFile());
    assertEquals(BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()),
        entry.getInodeFile().getId());
    assertEquals(3, entry.getInodeFile().getLength());
    assertEquals("test2", entry.getInodeFile().getName());

    Journal.JournalEntry entry2 = entries.get(1);
    assertNotNull(entry2.getUpdateInode());
    assertEquals(BlockId.createBlockId(3, BlockId.getMaxSequenceNumber()),
        entry2.getUpdateInode().getId());
    assertEquals("test3_unchanged", entry2.getUpdateInode().getName());

    Journal.JournalEntry entry3 = entries.get(2);
    assertNotNull(entry3.getUpdateInode());
    assertEquals(BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()),
        entry3.getUpdateInode().getId());
    assertEquals("test2_updated", entry3.getUpdateInode().getName());

    Journal.JournalEntry entry4 = entries.get(3);
    assertNotNull(entry4.getInodeFile());
    assertEquals(BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()),
        entry4.getInodeFile().getId());
    assertEquals(200, entry4.getInodeFile().getLength());
    assertEquals("test1", entry4.getInodeFile().getName());
  }

  @Test
  public void fileSystemMergeJournalContext() throws Exception {
    testMergeJournalContext(false);
  }

  @Test
  public void metadataSyncMergeJournalContext() throws Exception {
    testMergeJournalContext(true);
  }

  private void testMergeJournalContext(boolean useMetadataSyncJournalContext) throws Exception {
    JournalContext journalContext = Mockito.mock(JournalContext.class);
    List<Journal.JournalEntry> entries = new ArrayList<>();
    doAnswer(invocationOnMock -> {
      entries.add(invocationOnMock.getArgument(0));
      return null;
    }).when(journalContext).append(any(Journal.JournalEntry.class));

    doNothing().when(journalContext).flush();
    doNothing().when(journalContext).close();

    JournalContext mergeContext = useMetadataSyncJournalContext
        ? new MetadataSyncMergeJournalContext(journalContext, new FileSystemJournalEntryMerger()) :
        new FileSystemMergeJournalContext(journalContext, new FileSystemJournalEntryMerger());

    mergeContext.append(Journal.JournalEntry.newBuilder().getDefaultInstanceForType());
    assertEquals(0, entries.size());

    mergeContext.flush();
    // Flush should flush all journals held by the JournalContext into the writer
    assertEquals(1, entries.size());

    // Test merge journal entries. Detailed test can be found in FileSystemJournalEntryMergerTest
    mergeContext.append(Journal.JournalEntry.newBuilder().setInodeFile(
        File.InodeFileEntry.newBuilder().setId(
                BlockId.createBlockId(2, BlockId.getMaxSequenceNumber())).setLength(3)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test2").build()).build());

    mergeContext.append(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(
                BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()))
            .setName("test2_updated").build()).build());
    mergeContext.flush();
    assertEquals(2, entries.size());

    mergeContext.append(Journal.JournalEntry.newBuilder().getDefaultInstanceForType());
    mergeContext.close();
    // Close should also flush all journals held by the JournalContext into the writer
    assertEquals(3, entries.size());
  }
}
