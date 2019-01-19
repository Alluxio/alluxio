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

package alluxio.master.file.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.RpcContext;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.InodeStore.InodeStoreArgs;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.StreamUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link InodeTree}.
 */
public final class InodeTreeTest {
  private static final String TEST_PATH = "test";
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_DIR_URI = new AlluxioURI("/nested/test/dir");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final AlluxioURI NESTED_MULTIDIR_FILE_URI
      = new AlluxioURI("/nested/test/dira/dirb/file");
  public static final String TEST_OWNER = "user1";
  public static final String TEST_GROUP = "group1";
  public static final Mode TEST_DIR_MODE = new Mode((short) 0755);
  public static final Mode TEST_FILE_MODE = new Mode((short) 0644);
  private static CreateFileOptions sFileOptions;
  private static CreateDirectoryOptions sDirectoryOptions;
  private static CreateFileOptions sNestedFileOptions;
  private static CreateDirectoryOptions sNestedDirectoryOptions;
  private InodeStore mInodeStore;
  private InodeTree mTree;
  private MasterRegistry mRegistry;
  private MetricsMaster mMetricsMaster;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
          .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup")
          .build());

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    CoreMasterContext context = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, context);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, context);
    InodeDirectoryIdGenerator directoryIdGenerator =
        new InodeDirectoryIdGenerator(blockMaster);
    UfsManager ufsManager = mock(UfsManager.class);
    MountTable mountTable = new MountTable(ufsManager, mock(MountInfo.class));
    InodeLockManager lockManager = new InodeLockManager();
    mInodeStore = context.getInodeStoreFactory().apply(new InodeStoreArgs(lockManager));
    mTree = new InodeTree(mInodeStore, blockMaster, directoryIdGenerator, mountTable, lockManager);

    mRegistry.start(true);

    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE, NoopJournalContext.INSTANCE);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  /**
   * Sets up dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(TEST_OWNER)
        .setGroup(TEST_GROUP).setMode(TEST_FILE_MODE);
    sDirectoryOptions = CreateDirectoryOptions.defaults().setOwner(TEST_OWNER).setGroup(TEST_GROUP)
        .setMode(TEST_DIR_MODE);
    sNestedFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setOwner(TEST_OWNER).setGroup(TEST_GROUP).setMode(TEST_FILE_MODE).setRecursive(true);
    sNestedDirectoryOptions = CreateDirectoryOptions.defaults().setOwner(TEST_OWNER)
        .setGroup(TEST_GROUP).setMode(TEST_DIR_MODE).setRecursive(true);
  }

  /**
   * Tests that initializing the root twice results in the same root.
   */
  @Test
  public void initializeRootTwice() throws Exception {
    MutableInode<?> root = getInodeByPath(new AlluxioURI("/"));
    // initializeRoot call does nothing
    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE, NoopJournalContext.INSTANCE);
    assertEquals(TEST_OWNER, root.getOwner());
    MutableInode<?> newRoot = getInodeByPath(new AlluxioURI("/"));
    assertEquals(root, newRoot);
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathOptions)}
   * method for creating directories.
   */
  @Test
  public void createDirectory() throws Exception {
    // create directory
    createPath(mTree, TEST_URI, sDirectoryOptions);
    assertTrue(mTree.inodePathExists(TEST_URI));
    MutableInode<?> test = getInodeByPath(TEST_URI);
    assertEquals(TEST_PATH, test.getName());
    assertTrue(test.isDirectory());
    assertEquals("user1", test.getOwner());
    assertEquals("group1", test.getGroup());
    assertEquals(TEST_DIR_MODE.toShort(), test.getMode());

    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    assertTrue(mTree.inodePathExists(NESTED_URI));
    MutableInode<?> nested = getInodeByPath(NESTED_URI);
    assertEquals(TEST_PATH, nested.getName());
    assertEquals(2, nested.getParentId());
    assertTrue(test.isDirectory());
    assertEquals("user1", test.getOwner());
    assertEquals("group1", test.getGroup());
    assertEquals(TEST_DIR_MODE.toShort(), test.getMode());
  }

  /**
   * Tests that an exception is thrown when trying to create an already existing directory with the
   * {@code allowExists} flag set to {@code false}.
   */
  @Test
  public void createExistingDirectory() throws Exception {
    // create directory
    createPath(mTree, TEST_URI, sDirectoryOptions);

    // create again with allowExists true
    createPath(mTree, TEST_URI, CreateDirectoryOptions.defaults().setAllowExists(true));

    // create again with allowExists false
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(TEST_URI));
    createPath(mTree, TEST_URI, CreateDirectoryOptions.defaults().setAllowExists(false));
  }

  /**
   * Tests that creating a file under a pinned directory works.
   */
  @Test
  public void createFileUnderPinnedDirectory() throws Exception {
    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.WRITE_INODE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true, 0);
    }

    // create nested file under pinned folder
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // the nested file is pinned
    assertEquals(1, mTree.getPinIdSet().size());
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathOptions)}
   * method for creating a file.
   */
  @Test
  public void createFile() throws Exception {
    // created nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    MutableInode<?> nestedFile = getInodeByPath(NESTED_FILE_URI);
    assertEquals("file", nestedFile.getName());
    assertEquals(2, nestedFile.getParentId());
    assertTrue(nestedFile.isFile());
    assertEquals("user1", nestedFile.getOwner());
    assertEquals("group1", nestedFile.getGroup());
    assertEquals(TEST_FILE_MODE.toShort(), nestedFile.getMode());
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathOptions)}
   * method.
   */
  @Test
  public void createPathTest() throws Exception {
    // save the last mod time of the root
    long lastModTime = mTree.getRoot().getLastModificationTimeMs();
    // sleep to ensure a different last modification time
    CommonUtils.sleepMs(10);

    // Need to use updated options to set the correct last mod time.
    CreateDirectoryOptions dirOptions = CreateDirectoryOptions.defaults().setOwner(TEST_OWNER)
        .setGroup(TEST_GROUP).setMode(TEST_DIR_MODE).setRecursive(true);

    // create nested directory
    List<Inode> created = createPath(mTree, NESTED_URI, dirOptions);

    // 1 modified directory
    assertNotEquals(lastModTime,
        getInodeByPath(NESTED_URI.getParent()).getLastModificationTimeMs());
    // 2 created directories
    assertEquals(2, created.size());
    assertEquals("nested", created.get(0).getName());
    assertEquals("test", created.get(1).getName());
    // save the last mod time of 'test'
    lastModTime = created.get(1).getLastModificationTimeMs();
    // sleep to ensure a different last modification time
    CommonUtils.sleepMs(10);

    // creating the directory path again results in no new inodes.
    try {
      createPath(mTree, NESTED_URI, dirOptions);
      assertTrue("createPath should throw FileAlreadyExistsException", false);
    } catch (FileAlreadyExistsException e) {
      assertEquals(e.getMessage(),
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(NESTED_URI));
    }

    // create a file
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    created = createPath(mTree, NESTED_FILE_URI, options);

    // test directory was modified
    assertNotEquals(lastModTime, getInodeByPath(NESTED_URI).getLastModificationTimeMs());
    // file was created
    assertEquals(1, created.size());
    assertEquals("file", created.get(0).getName());
  }

  /**
   * Tests that an exception is thrown when trying to create the root path twice.
   */
  @Test
  public void createRootPath() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/");

    createPath(mTree, new AlluxioURI("/"), sFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with invalid block size.
   */
  @Test
  public void createFileWithInvalidBlockSize() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size 0");

    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(0);
    createPath(mTree, TEST_URI, options);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with a negative block size.
   */
  @Test
  public void createFileWithNegativeBlockSize() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size -1");

    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(-1);
    createPath(mTree, TEST_URI, options);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a non-existing directory.
   */
  @Test
  public void createFileUnderNonexistingDir() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("File /nested/test creation failed. Component 1(nested) does not exist");

    createPath(mTree, NESTED_URI, sFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file twice.
   */
  @Test
  public void createFileTwice() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/nested/test");

    createPath(mTree, NESTED_URI, sNestedFileOptions);
    createPath(mTree, NESTED_URI, sNestedFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a file path.
   */
  @Test
  public void createFileUnderFile() throws Exception {
    createPath(mTree, NESTED_URI, sNestedFileOptions);

    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed for path /nested/test/test. "
        + "Component 2(test) is a file, not a directory");
    createPath(mTree, new AlluxioURI("/nested/test/test"), sNestedFileOptions);
  }

  /**
   * Tests {@link InodeTree#inodeIdExists(long)}.
   */
  @Test
  public void inodeIdExists() throws Exception {
    assertTrue(mTree.inodeIdExists(0));
    assertFalse(mTree.inodeIdExists(1));

    createPath(mTree, TEST_URI, sFileOptions);
    MutableInode<?> inode = getInodeByPath(TEST_URI);
    assertTrue(mTree.inodeIdExists(inode.getId()));

    deleteInodeByPath(mTree, TEST_URI);
    assertFalse(mTree.inodeIdExists(inode.getId()));
  }

  /**
   * Tests {@link InodeTree#inodePathExists(AlluxioURI)}.
   */
  @Test
  public void inodePathExists() throws Exception {
    assertFalse(mTree.inodePathExists(TEST_URI));

    createPath(mTree, TEST_URI, sFileOptions);
    assertTrue(mTree.inodePathExists(TEST_URI));

    deleteInodeByPath(mTree, TEST_URI);
    assertFalse(mTree.inodePathExists(TEST_URI));
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing path.
   */
  @Test
  public void getInodeByNonexistingPath() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Path \"/test\" does not exist");

    assertFalse(mTree.inodePathExists(TEST_URI));
    getInodeByPath(TEST_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing, nested path.
   */
  @Test
  public void getInodeByNonexistingNestedPath() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Path \"/nested/test/file\" does not exist");

    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    assertFalse(mTree.inodePathExists(NESTED_FILE_URI));
    getInodeByPath(NESTED_FILE_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode with an invalid id.
   */
  @Test
  public void getInodeByInvalidId() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(1));

    assertFalse(mTree.inodeIdExists(1));
    try (LockedInodePath inodePath = mTree.lockFullInodePath(1, LockPattern.READ)) {
      // inode exists
    }
  }

  /**
   * Tests the {@link InodeTree#isRootId(long)} method.
   */
  @Test
  public void isRootId() {
    assertTrue(mTree.isRootId(0));
    assertFalse(mTree.isRootId(1));
  }

  /**
   * Tests the {@link InodeTree#getPath(InodeView)} method.
   */
  @Test
  public void getPath() throws Exception {
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, LockPattern.READ)) {
      Inode root = inodePath.getInode();
      // test root path
      assertEquals(new AlluxioURI("/"), mTree.getPath(root));
    }

    // test one level
    createPath(mTree, TEST_URI, sDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(TEST_URI, LockPattern.READ)) {
      assertEquals(new AlluxioURI("/test"), mTree.getPath(inodePath.getInode()));
    }

    // test nesting
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.READ)) {
      assertEquals(new AlluxioURI("/nested/test"), mTree.getPath(inodePath.getInode()));
    }
  }

  @Test
  public void getInodeChildrenRecursive() throws Exception {
    createPath(mTree, TEST_URI, sDirectoryOptions);
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    // add nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, LockPattern.WRITE_INODE)) {
      // /test, /nested, /nested/test, /nested/test/file
      assertEquals(4, mTree.getImplicitlyLockedDescendants(inodePath).size());
    }
  }

  /**
   * Tests deleting a nested inode.
   */
  @Test
  public void deleteInode() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, LockPattern.WRITE_INODE)) {
      // /nested, /nested/test
      assertEquals(2, mTree.getImplicitlyLockedDescendants(inodePath).size());

      // delete the nested inode
      deleteInodeByPath(mTree, NESTED_URI);

      // only /nested left
      assertEquals(1, mTree.getImplicitlyLockedDescendants(inodePath).size());
    }
  }

  @Test
  public void setPinned() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // no inodes pinned
    assertEquals(0, mTree.getPinIdSet().size());

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.WRITE_INODE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true, 0);
    }
    // nested file pinned
    assertEquals(1, mTree.getPinIdSet().size());

    // unpin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.WRITE_INODE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, false, 0);
    }
    assertEquals(0, mTree.getPinIdSet().size());
  }

  /**
   * Tests that streaming to a journal checkpoint works.
   */
  @Test
  public void streamToJournalCheckpoint() throws Exception {
    verifyJournal(mTree, Arrays.asList(getInodeByPath("/")));

    // test nested URI
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    verifyJournal(mTree, StreamUtils.map(path -> getInodeByPath(path),
        Arrays.asList("/", "/nested", "/nested/test", "/nested/test/file")));

    // add a sibling of test and verify journaling is in correct order (breadth first)
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    verifyJournal(mTree, StreamUtils.map(path -> getInodeByPath(path), Arrays.asList("/",
        "/nested", "/nested/test", "/nested/test1", "/nested/test/file", "/nested/test1/file1")));
  }

  @Test
  public void addInodeFromJournal() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    MutableInode<?> root = getInodeByPath("/");
    MutableInode<?> nested = getInodeByPath("/nested");
    MutableInode<?> test = getInodeByPath("/nested/test");
    MutableInode<?> file = getInodeByPath("/nested/test/file");
    MutableInode<?> test1 = getInodeByPath("/nested/test1");
    MutableInode<?> file1 = getInodeByPath("/nested/test1/file1");
    // reset the tree
    mTree.replayJournalEntryFromJournal(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(new AlluxioURI("/"), LockPattern.WRITE_INODE)) {
      assertEquals(0, mTree.getImplicitlyLockedDescendants(inodePath).size());
      mTree.replayJournalEntryFromJournal(nested.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested"));
      mTree.replayJournalEntryFromJournal(test.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test"));
      mTree.replayJournalEntryFromJournal(test1.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1"));
      mTree.replayJournalEntryFromJournal(file.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1", "file"));
      mTree.replayJournalEntryFromJournal(file1.toJournalEntry());
      verifyChildrenNames(mTree, inodePath,
          Sets.newHashSet("nested", "test", "test1", "file", "file1"));
    }
  }

  @Test
  public void addInodeModeFromJournalWithEmptyOwnership() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    MutableInode<?> root = getInodeByPath("/");
    MutableInode<?> nested = getInodeByPath("/nested");
    MutableInode<?> test = getInodeByPath("/nested/test");
    MutableInode<?> file = getInodeByPath("/nested/test/file");
    MutableInode<?>[] inodeChildren = {nested, test, file};
    for (MutableInode<?> child : inodeChildren) {
      child.setOwner("");
      child.setGroup("");
      child.setMode((short) 0600);
    }
    // reset the tree
    mTree.replayJournalEntryFromJournal(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(new AlluxioURI("/"), LockPattern.WRITE_INODE)) {
      assertEquals(0, mTree.getImplicitlyLockedDescendants(inodePath).size());
      mTree.replayJournalEntryFromJournal(nested.toJournalEntry());
      mTree.replayJournalEntryFromJournal(test.toJournalEntry());
      mTree.replayJournalEntryFromJournal(file.toJournalEntry());
      List<LockedInodePath> descendants = mTree.getImplicitlyLockedDescendants(inodePath);
      assertEquals(inodeChildren.length, descendants.size());
      for (LockedInodePath childPath : descendants) {
        Inode child = childPath.getInodeOrNull();
        Assert.assertNotNull(child);
        Assert.assertEquals("", child.getOwner());
        Assert.assertEquals("", child.getGroup());
        Assert.assertEquals((short) 0600, child.getMode());
      }
    }
  }

  @Test
  public void getInodePathById() throws Exception {
    try (LockedInodePath rootPath = mTree.lockFullInodePath(0, LockPattern.READ)) {
      assertEquals(0, rootPath.getInode().getId());
    }

    List<Inode> created = createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    for (Inode inode : created) {
      long id = inode.getId();
      try (LockedInodePath inodePath = mTree.lockFullInodePath(id, LockPattern.READ)) {
        assertEquals(id, inodePath.getInode().getId());
      }
    }
  }

  @Test
  public void getInodePathByPath() throws Exception {
    try (LockedInodePath rootPath =
        mTree.lockFullInodePath(new AlluxioURI("/"), LockPattern.READ)) {
      assertTrue(mTree.isRootId(rootPath.getInode().getId()));
    }

    // Create a nested file.
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    AlluxioURI uri = new AlluxioURI("/nested");
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, LockPattern.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, LockPattern.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_FILE_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, LockPattern.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }
  }

  // Helper to create a path.
  private List<Inode> createPath(InodeTree root, AlluxioURI path, CreatePathOptions<?> options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    try (LockedInodePath inodePath = root.lockInodePath(path, LockPattern.WRITE_EDGE)) {
      return root.createPath(RpcContext.NOOP, inodePath, options);
    }
  }

  // Helper to get an inode by path. The inode is unlocked before returning.
  private MutableInode<?> getInodeByPath(String path) {
    try {
      return getInodeByPath(new AlluxioURI(path));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Helper to get an inode by path. The inode is unlocked before returning.
  private MutableInode<?> getInodeByPath(AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = mTree.lockFullInodePath(path, LockPattern.READ)) {
      return mInodeStore.getMutable(inodePath.getInode().getId()).get();
    }
  }

  // Helper to delete an inode by path.
  private static void deleteInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = root.lockFullInodePath(path, LockPattern.WRITE_EDGE)) {
      root.deleteInode(RpcContext.NOOP, inodePath, System.currentTimeMillis());
    }
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<MutableInode<?>> journaled) {
    Iterator<alluxio.proto.journal.Journal.JournalEntry> it = root.getJournalEntryIterator();
    for (MutableInode<?> node : journaled) {
      assertTrue(it.hasNext());
      assertEquals(node.toJournalEntry(), it.next());
    }
    assertTrue(!it.hasNext());
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, LockedInodePath inodePath,
      Set<String> childNames) throws Exception {
    List<LockedInodePath> descendants = tree.getImplicitlyLockedDescendants(inodePath);
    assertEquals(childNames.size(), descendants.size());
    for (LockedInodePath childPath : descendants) {
      assertTrue(childNames.contains(childPath.getInode().getName()));
    }
  }
}
