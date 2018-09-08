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
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.RpcContext;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import java.util.stream.Collectors;

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
    MasterContext context = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, context);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, context);
    InodeDirectoryIdGenerator directoryIdGenerator =
        new InodeDirectoryIdGenerator(blockMaster);
    mTree = new InodeTree(blockMaster, directoryIdGenerator, mock(MountResolver.class));

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
    InodeView root = getInodeByPath(mTree, new AlluxioURI("/"));
    // initializeRoot call does nothing
    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE, NoopJournalContext.INSTANCE);
    assertEquals(TEST_OWNER, root.getOwner());
    InodeView newRoot = getInodeByPath(mTree, new AlluxioURI("/"));
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
    InodeView test = getInodeByPath(mTree, TEST_URI);
    assertEquals(TEST_PATH, test.getName());
    assertTrue(test.isDirectory());
    assertEquals("user1", test.getOwner());
    assertEquals("group1", test.getGroup());
    assertEquals(TEST_DIR_MODE.toShort(), test.getMode());

    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    assertTrue(mTree.inodePathExists(NESTED_URI));
    InodeView nested = getInodeByPath(mTree, NESTED_URI);
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
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.WRITE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true);
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
    InodeView nestedFile = getInodeByPath(mTree, NESTED_FILE_URI);
    assertEquals("file", nestedFile.getName());
    assertEquals(2, nestedFile.getParentId());
    assertTrue(nestedFile.isFile());
    assertEquals("user1", nestedFile.getOwner());
    assertEquals("group1", nestedFile.getGroup());
    assertEquals(TEST_FILE_MODE.toShort(), nestedFile.getMode());
  }

  /**
   * Tests the createPath method, specifically for ACLs and default ACLs.
   */
  @Test
  public void createPathAclTest() throws Exception {
    CreateDirectoryOptions dirOptions = CreateDirectoryOptions.defaults().setOwner(TEST_OWNER)
        .setGroup(TEST_GROUP).setMode(TEST_DIR_MODE).setRecursive(true);
    // create nested directory
    InodeTree.CreatePathResult createResult = createPath(mTree, NESTED_URI, dirOptions);
    List<InodeView> created = createResult.getCreated();
    // 2 created directories
    assertEquals(2, created.size());
    assertEquals("nested", created.get(0).getName());
    assertEquals("test", created.get(1).getName());
    DefaultAccessControlList dAcl = new DefaultAccessControlList(created.get(1).getACL());
    dAcl.setEntry(AclEntry.fromCliString("default:user::r-x"));
    ((Inode<?>) created.get(1)).setDefaultACL(dAcl);

    // create nested directory
    createResult = createPath(mTree, NESTED_DIR_URI, dirOptions);
    created = createResult.getCreated();
    // the new directory should have the same ACL as its parent
    // 1 created directories
    assertEquals(1, created.size());
    assertEquals("dir", created.get(0).getName());
    DefaultAccessControlList childDefaultAcl =  created.get(0).getDefaultACL();
    AccessControlList childAcl = created.get(0).getACL();
    assertEquals(dAcl, childDefaultAcl);

    assertEquals(childAcl.toStringEntries().stream().map(AclEntry::toDefault)
        .collect(Collectors.toList()), dAcl.toStringEntries());
    // create nested file
    createResult = createPath(mTree, NESTED_FILE_URI, dirOptions);
    created = createResult.getCreated();
    // the new file should have the same ACL as its parent's default ACL
    // 1 created file
    assertEquals(1, created.size());
    assertEquals("file", created.get(0).getName());
    childAcl = created.get(0).getACL();
    assertEquals(childAcl.toStringEntries().stream().map(AclEntry::toDefault)
        .collect(Collectors.toList()), dAcl.toStringEntries());

    // create nested directory
    createResult = createPath(mTree, NESTED_MULTIDIR_FILE_URI, dirOptions);
    created = createResult.getCreated();
    // 3 created directories
    assertEquals(3, created.size());
    assertEquals("dira", created.get(0).getName());
    assertEquals("dirb", created.get(1).getName());
    assertEquals("file", created.get(2).getName());

    for (InodeView inode: created) {
      childAcl = inode.getACL();
      // All the newly created directories and files should inherit the default ACL from parent
      assertEquals(childAcl.toStringEntries().stream().map(AclEntry::toDefault)
          .collect(Collectors.toList()), dAcl.toStringEntries());
    }

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
    InodeTree.CreatePathResult createResult = createPath(mTree, NESTED_URI, dirOptions);
    List<InodeView> modified = createResult.getModified();
    List<InodeView> created = createResult.getCreated();

    // 1 modified directory
    assertEquals(1, modified.size());
    assertEquals("", modified.get(0).getName());
    assertNotEquals(lastModTime, modified.get(0).getLastModificationTimeMs());
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
    createResult = createPath(mTree, NESTED_FILE_URI, options);
    modified = createResult.getModified();
    created = createResult.getCreated();

    // test directory was modified
    assertEquals(1, modified.size());
    assertEquals("test", modified.get(0).getName());
    assertNotEquals(lastModTime, modified.get(0).getLastModificationTimeMs());
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
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed. Component 2(test) is a file");

    createPath(mTree, NESTED_URI, sNestedFileOptions);
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
    InodeView inode = getInodeByPath(mTree, TEST_URI);
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
    mThrown.expectMessage("Path /test does not exist");

    assertFalse(mTree.inodePathExists(TEST_URI));
    getInodeByPath(mTree, TEST_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing, nested path.
   */
  @Test
  public void getInodeByNonexistingNestedPath() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Path /nested/test/file does not exist");

    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    assertFalse(mTree.inodePathExists(NESTED_FILE_URI));
    getInodeByPath(mTree, NESTED_FILE_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode with an invalid id.
   */
  @Test
  public void getInodeByInvalidId() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(1));

    assertFalse(mTree.inodeIdExists(1));
    try (LockedInodePath inodePath = mTree.lockFullInodePath(1, InodeTree.LockMode.READ)) {
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
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.READ)) {
      InodeView root = inodePath.getInode();
      // test root path
      assertEquals(new AlluxioURI("/"), mTree.getPath(root));
    }

    // test one level
    createPath(mTree, TEST_URI, sDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(TEST_URI, InodeTree.LockMode.READ)) {
      assertEquals(new AlluxioURI("/test"), mTree.getPath(inodePath.getInode()));
    }

    // test nesting
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.READ)) {
      assertEquals(new AlluxioURI("/nested/test"), mTree.getPath(inodePath.getInode()));
    }
  }

  /**
   * Tests the {@link InodeTree#lockDescendants(LockedInodePath, InodeTree.LockMode)} method.
   */
  @Test
  public void getInodeChildrenRecursive() throws Exception {
    createPath(mTree, TEST_URI, sDirectoryOptions);
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    // add nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.READ);
      LockedInodePathList lockedInodePathList = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.READ)) {
      // /test, /nested, /nested/test, /nested/test/file
      assertEquals(4, lockedInodePathList.getInodePathList().size());
    }
  }

  /**
   * Tests deleting a nested inode.
   */
  @Test
  public void deleteInode() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.WRITE);
      LockedInodePathList lockedInodePathList = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.READ)) {
      // /nested, /nested/test
      assertEquals(2, lockedInodePathList.getInodePathList().size());

      // delete the nested inode
      deleteInodeByPath(mTree, NESTED_URI);

      try (LockedInodePathList lockedInodePathList2 = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.WRITE)) {
        // only /nested left
        assertEquals(1, lockedInodePathList2.getInodePathList().size());
      }
    }
  }

  /**
   * Tests the {@link InodeTree#setPinned(RpcContext, LockedInodePath, boolean)} method.
   */
  @Test
  public void setPinned() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // no inodes pinned
    assertEquals(0, mTree.getPinIdSet().size());

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.WRITE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true);
    }
    // nested file pinned
    assertEquals(1, mTree.getPinIdSet().size());

    // unpin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.WRITE)) {
      mTree.setPinned(RpcContext.NOOP, inodePath, false);
    }
    assertEquals(0, mTree.getPinIdSet().size());
  }

  /**
   * Tests that streaming to a journal checkpoint works.
   */
  @Test
  public void streamToJournalCheckpoint() throws Exception {
    InodeDirectoryView root = mTree.getRoot();

    // test root
    verifyJournal(mTree, Lists.<InodeView>newArrayList(root));

    // test nested URI
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    InodeView file = test.getChild("file");
    verifyJournal(mTree, Arrays.asList(root, nested, test, file));

    // add a sibling of test and verify journaling is in correct order (breadth first)
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    InodeView file1 = test1.getChild("file1");
    verifyJournal(mTree, Arrays.asList(root, nested, test, test1, file, file1));
  }

  @Test
  public void addInodeFromJournal() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectoryView root = mTree.getRoot();
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    InodeView file = test.getChild("file");
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    InodeView file1 = test1.getChild("file1");
    // reset the tree
    mTree.replayJournalEntryFromJournal(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ);
         LockedInodePathList lockedInodePathList = mTree.lockDescendants(inodePath,
             InodeTree.LockMode.READ)) {
      assertEquals(0, lockedInodePathList.getInodePathList().size());
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
    InodeDirectoryView root = mTree.getRoot();
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    Inode<?> file = (Inode<?>) test.getChild("file");
    Inode<?>[] inodeChildren = {nested, test, file};
    for (Inode<?> child : inodeChildren) {
      child.setOwner("");
      child.setGroup("");
      child.setMode((short) 0600);
    }
    // reset the tree
    mTree.replayJournalEntryFromJournal(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ);
         LockedInodePathList lockedInodePathList = mTree.lockDescendants(inodePath,
             InodeTree.LockMode.READ)) {
      assertEquals(0, lockedInodePathList.getInodePathList().size());
      mTree.replayJournalEntryFromJournal(nested.toJournalEntry());
      mTree.replayJournalEntryFromJournal(test.toJournalEntry());
      mTree.replayJournalEntryFromJournal(file.toJournalEntry());
      try (LockedInodePathList descendants = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.READ)) {
        assertEquals(inodeChildren.length, descendants.getInodePathList().size());
        for (LockedInodePath childPath : descendants.getInodePathList()) {
          InodeView child = childPath.getInodeOrNull();
          Assert.assertNotNull(child);
          Assert.assertEquals("", child.getOwner());
          Assert.assertEquals("", child.getGroup());
          Assert.assertEquals((short) 0600, child.getMode());
        }
      }
    }
  }

  @Test
  public void getInodePathById() throws Exception {
    try (LockedInodePath rootPath = mTree.lockFullInodePath(0, InodeTree.LockMode.READ)) {
      assertEquals(0, rootPath.getInode().getId());
    }

    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    for (InodeView inode : createResult.getCreated()) {
      long id = inode.getId();
      try (LockedInodePath inodePath = mTree.lockFullInodePath(id, InodeTree.LockMode.READ)) {
        assertEquals(id, inodePath.getInode().getId());
      }
    }
  }

  @Test
  public void getInodePathByPath() throws Exception {
    try (LockedInodePath rootPath =
        mTree.lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ)) {
      assertTrue(mTree.isRootId(rootPath.getInode().getId()));
    }

    // Create a nested file.
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    AlluxioURI uri = new AlluxioURI("/nested");
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_FILE_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }
  }

  @Test
  public void lockingDescendent() throws Exception {
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    InodeView dirInode = createResult.getCreated().get(0);
    assertTrue(dirInode.isDirectory());
    try (LockedInodePath lockedDirPath = mTree.lockFullInodePath(dirInode.getId(),
         InodeTree.LockMode.READ);
         LockedInodePath path = mTree.lockDescendantPath(lockedDirPath,
             InodeTree.LockMode.READ, NESTED_FILE_URI);
        ) {
      assertEquals(4, path.getInodeList().size());
    }
    // Testing descendant is the same as the LockedInodePath.
    try (LockedInodePath lockedDirPath = mTree.lockFullInodePath(dirInode.getId(),
        InodeTree.LockMode.READ);
         LockedInodePath path = mTree.lockDescendantPath(lockedDirPath,
             InodeTree.LockMode.READ, lockedDirPath.getUri())
    ) {
      Assert.fail();
    } catch (InvalidPathException e) {
      // expected
    }

    // Testing descendantURI is actually the URI of a parent.
    InodeView subDirInode = createResult.getCreated().get(1);
    assertTrue(dirInode.isDirectory());
    try (LockedInodePath lockedDirPath = mTree.lockFullInodePath(subDirInode.getId(),
        InodeTree.LockMode.READ);
         LockedInodePath path = mTree.lockDescendantPath(lockedDirPath,
             InodeTree.LockMode.READ, new AlluxioURI(""));
    ) {
      Assert.fail();
    } catch (InvalidPathException e) {
      // expected
    }
  }

  // Helper to create a path.
  private InodeTree.CreatePathResult createPath(InodeTree root, AlluxioURI path,
      CreatePathOptions<?> options) throws FileAlreadyExistsException, BlockInfoException,
      InvalidPathException, IOException, FileDoesNotExistException {
    try (LockedInodePath inodePath = root.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      return root.createPath(RpcContext.NOOP, inodePath, options);
    }
  }

  // Helper to get an inode by path. The inode is unlocked before returning.
  private static InodeView getInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = root.lockFullInodePath(path, InodeTree.LockMode.READ)) {
      return inodePath.getInode();
    }
  }

  // Helper to delete an inode by path.
  private static void deleteInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = root.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      root.deleteInode(RpcContext.NOOP, inodePath, System.currentTimeMillis());
    }
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<InodeView> journaled) throws Exception {
    Iterator<alluxio.proto.journal.Journal.JournalEntry> it = root.getJournalEntryIterator();
    for (InodeView node : journaled) {
      assertTrue(it.hasNext());
      assertEquals(node.toJournalEntry(), it.next());
    }
    assertTrue(!it.hasNext());
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, LockedInodePath inodePath,
      Set<String> childNames) throws Exception {
    try (LockedInodePathList childrenPath = tree.lockDescendants(inodePath,
        InodeTree.LockMode.READ)) {
      assertEquals(childNames.size(), childrenPath.getInodePathList().size());
      for (LockedInodePath childPath : childrenPath.getInodePathList()) {
        assertTrue(childNames.contains(childPath.getInode().getName()));
      }
    }
  }
}
