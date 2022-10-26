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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.contexts.InternalOperationContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.journal.FileSystemMergeJournalContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.MetadataSyncMergeJournalContext;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.Factory.class})
public class FileSystemMasterSyncMetadataFlushJournalTest
    extends FileSystemMasterSyncMetadataTestBase {
  public FileSystemMasterSyncMetadataFlushJournalTest() {
  }

  @Override
  public void before() throws Exception {
    super.before();

    // verify the files don't exist in alluxio
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  @Test
  public void hierarchicalDirectoryMerge() throws Exception {
    run(3, 5, true);
  }

  @Test
  public void hierarchicalDirectoryNoMerge() throws Exception {
    run(3, 5, false);
  }

  @Test
  public void flatDirectoryMerge() throws Exception {
    run(1, 100, true);
  }

  @Test
  public void flatDirectoryNoMerge() throws Exception {
    run(1, 100, false);
  }

  @Test
  public void runFailedHierarchical()
      throws IOException, AccessControlException, InvalidPathException {
    Configuration.set(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, true);

    mUfs.mFailedPaths.clear();
    mUfs.mFailedPaths.add("0_1");

    cleanupUfs();
    createTestUfs(3, 5);

    TestInodeSyncStream iss;

    TestJournalContext testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.FAILED);
    }
    iss.assertAllJournalFlushedIntoAsyncJournalWriter();
  }

  @Test
  public void runFailedFlat()
      throws IOException {
    Configuration.set(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, true);

    mUfs.mFailedPaths.clear();
    mUfs.mFailedPaths.add("/");

    cleanupUfs();
    createTestUfs(1, 100);

    AtomicReference<TestInodeSyncStream> iss = new AtomicReference<>();

    TestJournalContext testJournalContext = new TestJournalContext();
    assertThrows(RuntimeException.class, () -> {
      try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
          testJournalContext,
          new FileSystemJournalEntryMerger()))) {
        iss.set(makeInodeSyncStream("/", journalContext));
        assertEquals(iss.get().sync(), InodeSyncStream.SyncStatus.FAILED);
      }
    });
    iss.get().assertAllJournalFlushedIntoAsyncJournalWriter();
  }

  private void run(int numLevels, int numInodesPerLevel, boolean mergeInodeJournals)
      throws Exception {
    Configuration.set(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, mergeInodeJournals);

    int current = 1;
    int sum = 0;
    for (int i = 0; i <= numLevels; ++i) {
      sum += current;
      current *= numInodesPerLevel;
    }
    int numExpectedInodes = sum;
    int numExpectedDirectories = sum - current / numInodesPerLevel;
    int numExpectedFiles = current / numInodesPerLevel;
    cleanupUfs();
    createTestUfs(numLevels, numInodesPerLevel);

    // Test load metadata from UFS
    TestInodeSyncStream iss;
    TestJournalContext testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.OK);
    }
    // A. 1 journal entry for an inode file
    // B. 2 to 3 journals for a directory
    //    a. generating inode directory id
    //    b. create inode file
    //    c. (maybe) set children loaded
    // C. at most num inode per level update the sync root inode last modified time
    assertEquals(numExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    assertTrue(testJournalContext.mAppendedEntries.size()
        >= numExpectedFiles + numExpectedDirectories * 2);
    assertTrue(testJournalContext.mAppendedEntries.size()
        <= numExpectedFiles + numExpectedDirectories * 3 + numInodesPerLevel);
    Set<MutableInode<?>> inodes = mFileSystemMaster.getInodeStore().allInodes();
    for (MutableInode<?> inode: inodes) {
      if (inode.isDirectory()) {
        assertTrue(inode.asDirectory().isDirectChildrenLoaded());
      }
      if (inode.isFile()) {
        assertTrue(inode.asFile().isCompleted());
      }
    }
    assertEquals(1, testJournalContext.mFlushCount.get());
    iss.assertAllJournalFlushedIntoAsyncJournalWriter();

    // Update inode metadata in UFS and the metadata sync should delete inodes and then create them
    cleanupUfs();
    createUfsHierarchy(0, numLevels, "", numInodesPerLevel);
    testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.OK);
    }
    assertEquals(numExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    assertEquals(numExpectedFiles * 2, testJournalContext.mAppendedEntries.size());
    assertEquals(1, testJournalContext.mFlushCount.get());
    iss.assertAllJournalFlushedIntoAsyncJournalWriter();

    // Delete inodes in UFS and trigger a metadata sync
    cleanupUfs();
    testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.OK);
    }
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());
    assertEquals(numExpectedInodes - 1, testJournalContext.mAppendedEntries.size());
    assertEquals(1, mFileSystemMaster.getInodeCount());
    assertEquals(1, testJournalContext.mFlushCount.get());
    iss.assertAllJournalFlushedIntoAsyncJournalWriter();
  }

  private TestInodeSyncStream makeInodeSyncStream(String path, JournalContext journalContext) {
    FileSystemMasterCommonPOptions options = FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(0)
        .build();
    DescendantType descendantType = DescendantType.ALL;
    try {
      LockingScheme syncScheme =
          new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, options,
              mFileSystemMaster.getSyncPathCache(), descendantType); // shouldSync
      return
          new TestInodeSyncStream(
              syncScheme, mFileSystemMaster, mFileSystemMaster.getSyncPathCache(),
              new RpcContext(NoopBlockDeletionContext.INSTANCE,
                  journalContext, new InternalOperationContext()), descendantType, options,
              false,
              false,
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createTestUfs(int numLevels, int numInodesPerLevel) throws IOException {
    createUfsHierarchy(0, numLevels, "", numInodesPerLevel);
  }

  private static class TestInodeSyncStream extends InodeSyncStream {
    private List<MetadataSyncMergeJournalContext> mJournalContexts = new ArrayList<>();

    public TestInodeSyncStream(
        LockingScheme rootScheme, DefaultFileSystemMaster fsMaster,
        UfsSyncPathCache syncPathCache, RpcContext rpcContext,
        DescendantType descendantType,
        FileSystemMasterCommonPOptions options,
        boolean forceSync, boolean loadOnly, boolean loadAlways) {
      super(rootScheme, fsMaster, syncPathCache, rpcContext, descendantType, options, forceSync,
          loadOnly, loadAlways);
    }

    @Override
    protected synchronized RpcContext getMetadataSyncRpcContext() {
      RpcContext context = super.getMetadataSyncRpcContext();
      if (context.getJournalContext() instanceof MetadataSyncMergeJournalContext) {
        mJournalContexts.add((MetadataSyncMergeJournalContext) context.getJournalContext());
      }
      return context;
    }

    public void assertAllJournalFlushedIntoAsyncJournalWriter() {
      for (MetadataSyncMergeJournalContext journalContext: mJournalContexts) {
        assertEquals(0, journalContext.getMerger().getMergedJournalEntries().size());
      }
    }
  }

  private static class TestJournalContext implements JournalContext {
    List<Journal.JournalEntry> mAppendedEntries = new ArrayList<>();
    List<Journal.JournalEntry> mPendingEntries = new ArrayList<>();
    AtomicInteger mFlushCount = new AtomicInteger();

    @Override
    public synchronized void append(Journal.JournalEntry entry) {
      mAppendedEntries.add(entry);
      mPendingEntries.add(entry);
    }

    @Override
    public synchronized void flush() throws UnavailableException {
      if (mPendingEntries.size() > 0) {
        mPendingEntries.clear();
        mFlushCount.incrementAndGet();
      }
    }

    @Override
    public synchronized void close() throws UnavailableException {
      flush();
    }
  }
}

