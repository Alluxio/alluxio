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
import java.util.Collections;
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
  public void hierarchicalDirectory() throws Exception {
    run(3, 5);
  }

  @Test
  public void flatDirectory() throws Exception {
    run(1, 100);
  }

  @Test
  public void runFailedHierarchical()
      throws IOException, AccessControlException, InvalidPathException {
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
      iss.assertAllJournalFlushedIntoAsyncJournalWriter();
    }
  }

  @Test
  public void runFailedFlat()
      throws IOException {
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
        iss.get().assertAllJournalFlushedIntoAsyncJournalWriter();
      }
    });
  }

  private void run(int numLevels, int numInodesPerLevel)
      throws Exception {
    int current = numInodesPerLevel;
    int sum = 0;
    for (int i = 0; i < numLevels; ++i) {
      sum += current;
      current *= numInodesPerLevel;
    }
    int numExpectedInodes = sum + 1;
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
      // No journal should be written once the metadata sync is done.
      testJournalContext.mAllowAppendingOrFlushingJournals = false;
      iss.assertAllJournalFlushedIntoAsyncJournalWriter();
    }
    // A. 1 journal entry for an inode file
    // B. 3-4 journals for a directory
    //    a. generating inode directory id
    //    b. create inode file
    //    c. set children loaded
    //    d. (maybe) update access time
    // C. 1-2 journals for root
    //    a. set children loaded
    //    b. (maybe) update access time
    assertEquals(numExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    System.out.println("yimin666");
    System.out.println(testJournalContext.mAppendedEntries.size());
    for (Journal.JournalEntry entry: testJournalContext.mAppendedEntries) {
      System.out.println(entry);
    }
    assertTrue(testJournalContext.mAppendedEntries.size()
        >= numExpectedFiles + numExpectedDirectories * 3);
    assertTrue(testJournalContext.mAppendedEntries.size()
        <= numExpectedFiles + numExpectedDirectories * 4 + 2);
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

    // Update inode metadata in UFS and the metadata sync should delete inodes and then create them
    cleanupUfs();
    createUfsHierarchy(0, numLevels, "", numInodesPerLevel);
    testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.OK);
      // No journal should be written once the metadata sync is done.
      testJournalContext.mAllowAppendingOrFlushingJournals = false;
      iss.assertAllJournalFlushedIntoAsyncJournalWriter();
    }
    assertEquals(numExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    System.out.println("yimin777");
    System.out.println(testJournalContext.mAppendedEntries.size());
    for (Journal.JournalEntry entry: testJournalContext.mAppendedEntries) {
      System.out.println(entry);
    }
    assertEquals(numExpectedFiles * 2, testJournalContext.mAppendedEntries.size());
    assertEquals(1, testJournalContext.mFlushCount.get());

    // Delete inodes in UFS and trigger a metadata sync
    cleanupUfs();
    testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger()))) {
      iss = makeInodeSyncStream("/", journalContext);
      assertEquals(iss.sync(), InodeSyncStream.SyncStatus.OK);
      // No journal should be written once the metadata sync is done.
      testJournalContext.mAllowAppendingOrFlushingJournals = false;
      iss.assertAllJournalFlushedIntoAsyncJournalWriter();
    }
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());
    System.out.println("yimin888");
    System.out.println(testJournalContext.mAppendedEntries.size());
    for (Journal.JournalEntry entry: testJournalContext.mAppendedEntries) {
      System.out.println(entry);
    }
    assertEquals(numExpectedInodes - 1, testJournalContext.mAppendedEntries.size());
    assertEquals(1, mFileSystemMaster.getInodeCount());
    assertEquals(1, testJournalContext.mFlushCount.get());
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
    List<Journal.JournalEntry> mAppendedEntries = Collections.synchronizedList(new ArrayList<>());
    List<Journal.JournalEntry> mPendingEntries = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger mFlushCount = new AtomicInteger();
    volatile boolean mAllowAppendingOrFlushingJournals = true;

    @Override
    public synchronized void append(Journal.JournalEntry entry) {
      assertTrue(mAllowAppendingOrFlushingJournals);
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

