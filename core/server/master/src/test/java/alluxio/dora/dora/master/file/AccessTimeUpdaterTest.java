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

package alluxio.dora.dora.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.dora.dora.AlluxioURI;
import alluxio.dora.dora.Constants;
import alluxio.dora.dora.conf.Configuration;
import alluxio.dora.dora.conf.PropertyKey;
import alluxio.dora.dora.master.CoreMasterContext;
import alluxio.dora.dora.master.MasterRegistry;
import alluxio.dora.dora.master.block.BlockMaster;
import alluxio.dora.dora.master.block.BlockMasterFactory;
import alluxio.dora.dora.master.file.contexts.CreateFileContext;
import alluxio.dora.dora.master.file.meta.options.MountInfo;
import alluxio.dora.dora.master.journal.*;
import alluxio.dora.dora.master.metastore.InodeStore;
import alluxio.dora.dora.master.metrics.MetricsMasterFactory;
import alluxio.dora.dora.security.authorization.Mode;
import alluxio.dora.dora.underfs.UfsManager;
import alluxio.dora.dora.util.CommonUtils;
import alluxio.dora.dora.util.executor.ControllableScheduler;
import alluxio.dora.dora.master.MasterTestUtils;
import alluxio.dora.dora.master.file.meta.Inode;
import alluxio.dora.dora.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.dora.dora.master.file.meta.InodeLockManager;
import alluxio.dora.dora.master.file.meta.InodeTree;
import alluxio.dora.dora.master.file.meta.LockedInodePath;
import alluxio.dora.dora.master.file.meta.MountTable;
import alluxio.dora.dora.master.file.meta.MutableInode;
import alluxio.dora.dora.master.journal.JournalContext;
import alluxio.dora.dora.master.journal.JournalSystem;
import alluxio.dora.dora.master.journal.JournalTestUtils;
import alluxio.dora.dora.master.journal.JournalType;
import alluxio.dora.dora.master.journal.NoopJournalContext;
import alluxio.proto.journal.Journal;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link PermissionChecker}.
 */
public final class AccessTimeUpdaterTest {
  private static final String TEST_OWNER = "user1";
  private static final String TEST_GROUP = "";
  private static final Mode TEST_MODE = new Mode((short) 0755);

  private ControllableScheduler mScheduler;
  private FileSystemMaster mFileSystemMaster;
  private AccessTimeUpdater mAccessTimeUpdater;
  private BlockMaster mBlockMaster;
  private InodeStore mInodeStore;
  private CoreMasterContext mContext;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  private InodeTree mInodeTree;

  @Before
  public final void before() throws Exception {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    PowerMockito.when(mFileSystemMaster.getName()).thenReturn(Constants.FILE_SYSTEM_MASTER_NAME);
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    MasterRegistry registry = new MasterRegistry();
    JournalSystem journalSystem = JournalTestUtils.createJournalSystem(mTestFolder);
    mContext = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(registry, mContext);
    mBlockMaster = new BlockMasterFactory().create(registry, mContext);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    UfsManager manager = mock(UfsManager.class);
    MountTable mountTable = new MountTable(manager, mock(MountInfo.class), Clock.systemUTC());
    InodeLockManager lockManager = new InodeLockManager();
    mInodeStore = mContext.getInodeStoreFactory().apply(lockManager);
    mInodeTree =
        new InodeTree(mInodeStore, mBlockMaster, directoryIdGenerator, mountTable, lockManager);

    journalSystem.start();
    journalSystem.gainPrimacy();
    mBlockMaster.start(true);

    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true);
    Configuration
        .set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup");
    mInodeTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_MODE, NoopJournalContext.INSTANCE);
    mScheduler = new ControllableScheduler();
  }

  private void createInode(String path, CreateFileContext context)
      throws Exception {
    try (LockedInodePath inodePath =
             mInodeTree.lockInodePath(
                 new AlluxioURI(path),
                 InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE
             )
    ) {
      List<Inode> result = mInodeTree.createPath(RpcContext.NOOP, inodePath, context);
      MutableInode<?> inode = mInodeStore.getMutable(result.get(result.size() - 1).getId()).get();
      mInodeStore.writeInode(inode);
    }
  }

  @Test
  public void updateAccessTimeImmediately() throws Exception {
    mAccessTimeUpdater = new AccessTimeUpdater(mFileSystemMaster, mInodeTree,
        mContext.getJournalSystem(), 0, 0, 0);
    mAccessTimeUpdater.start();
    String path = "/foo";
    JournalContext journalContext = mock(JournalContext.class);
    when(journalContext.get()).thenReturn(journalContext);
    createInode(path, CreateFileContext.defaults());
    long accessTime = CommonUtils.getCurrentMs() + 100L;
    long inodeId;
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), accessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify journal entry is logged
    ArgumentCaptor<Journal.JournalEntry> captor =
        ArgumentCaptor.forClass(Journal.JournalEntry.class);
    verify(journalContext).append(captor.capture());
    assertTrue(captor.getValue().hasUpdateInode());
    assertEquals(inodeId, captor.getValue().getUpdateInode().getId());
    assertEquals(accessTime, captor.getValue().getUpdateInode().getLastAccessTimeMs());

    // verify inode attribute is updated
    assertEquals(accessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());
  }

  @Test
  public void updateAccessTimeAsync() throws Exception {
    mAccessTimeUpdater = new AccessTimeUpdater(mFileSystemMaster, mInodeTree,
        mContext.getJournalSystem(), 10 * Constants.SECOND_MS, 0, 0);
    mAccessTimeUpdater.start(mScheduler);
    String path = "/foo";
    createInode(path, CreateFileContext.defaults());
    JournalContext journalContext = mock(JournalContext.class);
    when(journalContext.get()).thenReturn(journalContext);
    PowerMockito.when(mFileSystemMaster.createJournalContext()).thenReturn(journalContext);
    long accessTime = CommonUtils.getCurrentMs() + 100L;
    long inodeId;
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), accessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify inode attribute is updated
    assertEquals(accessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());

    mScheduler.jumpAndExecute(1, TimeUnit.SECONDS);

    // verify journal entry is NOT logged yet
    verify(journalContext, never()).append(any(Journal.JournalEntry.class));

    // wait for the flush to complete
    mScheduler.jumpAndExecute(11, TimeUnit.SECONDS);

    /// verify journal entry is logged after the flush interval
    ArgumentCaptor<Journal.JournalEntry> captor =
        ArgumentCaptor.forClass(Journal.JournalEntry.class);
    verify(journalContext).append(captor.capture());
    assertTrue(captor.getValue().hasUpdateInode());
    assertEquals(inodeId, captor.getValue().getUpdateInode().getId());
    assertEquals(accessTime, captor.getValue().getUpdateInode().getLastAccessTimeMs());
  }

  @Test
  public void updateAccessTimePrecision() throws Exception {
    mAccessTimeUpdater = new AccessTimeUpdater(mFileSystemMaster, mInodeTree,
        mContext.getJournalSystem(), 0, Constants.HOUR_MS, 0);
    mAccessTimeUpdater.start();
    String path = "/foo";
    createInode(path, CreateFileContext.defaults());
    JournalContext journalContext = mock(JournalContext.class);
    when(journalContext.get()).thenReturn(journalContext);
    PowerMockito.when(mFileSystemMaster.createJournalContext()).thenReturn(journalContext);
    long accessTime = CommonUtils.getCurrentMs() + 100L;
    long inodeId;
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), accessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify inode attribute is not updated
    assertNotEquals(accessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());

    // verify journal entry is not logged yet
    verify(journalContext, never()).append(any(Journal.JournalEntry.class));

    long newAccessTime = CommonUtils.getCurrentMs() + 2 * Constants.HOUR_MS;

    // update access time with a much later timestamp
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), newAccessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify inode attribute is updated
    assertEquals(newAccessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());
    /// verify journal entry is logged
    ArgumentCaptor<Journal.JournalEntry> captor =
        ArgumentCaptor.forClass(Journal.JournalEntry.class);
    verify(journalContext).append(captor.capture());
    assertTrue(captor.getValue().hasUpdateInode());
    assertEquals(inodeId, captor.getValue().getUpdateInode().getId());
    assertEquals(newAccessTime, captor.getValue().getUpdateInode().getLastAccessTimeMs());
  }

  @Test
  public void updateAccessTimePrecisionAsync() throws Exception {
    mAccessTimeUpdater = new AccessTimeUpdater(mFileSystemMaster, mInodeTree,
        mContext.getJournalSystem(), Constants.MINUTE_MS, Constants.HOUR_MS, 0);
    mAccessTimeUpdater.start(mScheduler);
    String path = "/foo";
    createInode(path, CreateFileContext.defaults());
    JournalContext journalContext = mock(JournalContext.class);
    when(journalContext.get()).thenReturn(journalContext);
    PowerMockito.when(mFileSystemMaster.createJournalContext()).thenReturn(journalContext);
    long accessTime = CommonUtils.getCurrentMs() + 100L;
    long inodeId;
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), accessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    mScheduler.jumpAndExecute(2, TimeUnit.MINUTES);

    // verify inode attribute is not updated
    assertNotEquals(accessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());

    // verify journal entry is not logged
    verify(journalContext, never()).append(any(Journal.JournalEntry.class));

    long newAccessTime = CommonUtils.getCurrentMs() + 2 * Constants.HOUR_MS;

    // update access time with a much later timestamp
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), newAccessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify inode attribute is updated
    assertEquals(newAccessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());

    mScheduler.jumpAndExecute(2, TimeUnit.SECONDS);

    // verify journal entry is not logged
    verify(journalContext, never()).append(any(Journal.JournalEntry.class));

    mScheduler.jumpAndExecute(2, TimeUnit.MINUTES);

    /// verify journal entry is logged after the flush interval
    ArgumentCaptor<Journal.JournalEntry> captor =
        ArgumentCaptor.forClass(Journal.JournalEntry.class);
    verify(journalContext).append(captor.capture());
    assertTrue(captor.getValue().hasUpdateInode());
    assertEquals(inodeId, captor.getValue().getUpdateInode().getId());
    assertEquals(newAccessTime, captor.getValue().getUpdateInode().getLastAccessTimeMs());
  }

  @Test
  public void updateAccessTimeAsyncOnShutdown() throws Exception {
    mAccessTimeUpdater = new AccessTimeUpdater(mFileSystemMaster, mInodeTree,
        mContext.getJournalSystem(), 10 * Constants.SECOND_MS, 0, 0);
    mAccessTimeUpdater.start(mScheduler);
    String path = "/foo";
    createInode(path, CreateFileContext.defaults());
    JournalContext journalContext = mock(JournalContext.class);
    when(journalContext.get()).thenReturn(journalContext);
    PowerMockito.when(mFileSystemMaster.createJournalContext()).thenReturn(journalContext);
    long accessTime = CommonUtils.getCurrentMs() + 100L;
    long inodeId;
    try (LockedInodePath lockedInodes = mInodeTree.lockFullInodePath(new AlluxioURI(path),
        InodeTree.LockPattern.READ, journalContext)) {
      mAccessTimeUpdater.updateAccessTime(journalContext, lockedInodes.getInode(), accessTime);
      inodeId = lockedInodes.getInode().getId();
    }

    // verify inode attribute is updated
    assertEquals(accessTime, mInodeStore.get(inodeId).get().getLastAccessTimeMs());

    mScheduler.jumpAndExecute(1, TimeUnit.SECONDS);

    // verify journal entry is NOT logged yet
    verify(journalContext, never()).append(any(Journal.JournalEntry.class));

    // wait for the flush to complete
    mContext.getJournalSystem().stop();

    /// verify journal entry is logged after the flush interval
    ArgumentCaptor<Journal.JournalEntry> captor =
        ArgumentCaptor.forClass(Journal.JournalEntry.class);
    verify(journalContext).append(captor.capture());
    assertTrue(captor.getValue().hasUpdateInode());
    assertEquals(inodeId, captor.getValue().getUpdateInode().getId());
    assertEquals(accessTime, captor.getValue().getUpdateInode().getLastAccessTimeMs());
  }
}
