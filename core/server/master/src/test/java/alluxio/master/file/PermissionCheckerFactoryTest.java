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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Clock;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PermissionCheckerFactory}.
 */
public final class PermissionCheckerFactoryTest {

    private static final String TEST_OWNER = "user1";
    private static final String TEST_GROUP = "";
    private static final Mode TEST_MODE = new Mode((short) 0755);
    private BlockMaster mBlockMaster;
    private InodeStore mInodeStore;
    private CoreMasterContext mContext;

    @Rule
    public TemporaryFolder mTestFolder = new TemporaryFolder();
    private InodeTree mInodeTree;

    @Before
    public void before() throws Exception {
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
        mInodeTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_MODE, NoopJournalContext.INSTANCE);
    }

    @Test
    public void testCreationWithDefault() throws Exception {
        Configuration.unset(PropertyKey.PERMISSION_CHECKER_CLASS);
        PermissionChecker checker = new PermissionCheckerFactory().create(mInodeTree);
        assertTrue(checker instanceof DefaultPermissionChecker);
    }

    @Test
    public void testCreationWithExtension() throws Exception {
        Configuration.set(PropertyKey.PERMISSION_CHECKER_CLASS, SamplePermissionChecker.class.getName());
        PermissionChecker checker = new PermissionCheckerFactory().create(mInodeTree);
        assertTrue(checker instanceof SamplePermissionChecker);
    }

    @Test
    public void testCreationWithNull() throws Exception {
        Configuration.set(PropertyKey.PERMISSION_CHECKER_CLASS, SamplePermissionChecker.class.getName());
        PermissionChecker checker = new PermissionCheckerFactory().create(null);
        assertTrue(checker instanceof SamplePermissionChecker);
    }

    @Test
    public void testCheckAccessAllowed() throws Exception {
        Configuration.set(PropertyKey.PERMISSION_CHECKER_CLASS, SamplePermissionChecker.class.getName());
        PermissionChecker checker = new PermissionCheckerFactory().create(mInodeTree);
        AlluxioURI uri = new AlluxioURI("/a/b/c");
        try (LockedInodePath inodePath =
                     mInodeTree.lockInodePath(
                             uri,
                             InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE
                     )
        ) {
            checker.checkPermission(Mode.Bits.ALL, inodePath);
        }
    }

    @Test(expected = AccessControlException.class)
    public void testCheckAccessDenied() throws Exception {
        Configuration.set(PropertyKey.PERMISSION_CHECKER_CLASS, SamplePermissionChecker.class.getName());
        PermissionChecker checker = new PermissionCheckerFactory().create(mInodeTree);
        AlluxioURI uri = new AlluxioURI("/x/y/z");
        try (LockedInodePath inodePath =
                     mInodeTree.lockInodePath(
                             uri,
                             InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE
                     )
        ) {
            checker.checkPermission(Mode.Bits.ALL, inodePath);
        }
    }
}
