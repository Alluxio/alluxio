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

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.NoopJournalContext;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
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
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
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
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
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
    JournalFactory factory =
        new Journal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));

    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, factory);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    MountTable mountTable = new MountTable(ufsManager);
    mTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    mRegistry.start(true);

    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE);
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
    Inode<?> root = getInodeByPath(mTree, new AlluxioURI("/"));
    // initializeRoot call does nothing
    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE);
    Assert.assertEquals(TEST_OWNER, root.getOwner());
    Inode<?> newRoot = getInodeByPath(mTree, new AlluxioURI("/"));
    Assert.assertEquals(root, newRoot);
  }

  /**
   * Tests the {@link InodeTree#createPath(LockedInodePath, CreatePathOptions)} method for creating
   * directories.
   */
  @Test
  public void createDirectory() throws Exception {
    // create directory
    createPath(mTree, TEST_URI, sDirectoryOptions);
    Assert.assertTrue(mTree.inodePathExists(TEST_URI));
    Inode<?> test = getInodeByPath(mTree, TEST_URI);
    Assert.assertEquals(TEST_PATH, test.getName());
    Assert.assertTrue(test.isDirectory());
    Assert.assertEquals("user1", test.getOwner());
    Assert.assertEquals("group1", test.getGroup());
    Assert.assertEquals(TEST_DIR_MODE.toShort(), test.getMode());

    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    Assert.assertTrue(mTree.inodePathExists(NESTED_URI));
    Inode<?> nested = getInodeByPath(mTree, NESTED_URI);
    Assert.assertEquals(TEST_PATH, nested.getName());
    Assert.assertEquals(2, nested.getParentId());
    Assert.assertTrue(test.isDirectory());
    Assert.assertEquals("user1", test.getOwner());
    Assert.assertEquals("group1", test.getGroup());
    Assert.assertEquals(TEST_DIR_MODE.toShort(), test.getMode());
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
      mTree.setPinned(inodePath, true);
    }

    // create nested file under pinned folder
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // the nested file is pinned
    Assert.assertEquals(1, mTree.getPinIdSet().size());
  }

  /**
   * Tests the {@link InodeTree#createPath(LockedInodePath, CreatePathOptions)} method for creating
   * a file.
   */
  @Test
  public void createFile() throws Exception {
    // created nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    Inode<?> nestedFile = getInodeByPath(mTree, NESTED_FILE_URI);
    Assert.assertEquals("file", nestedFile.getName());
    Assert.assertEquals(2, nestedFile.getParentId());
    Assert.assertTrue(nestedFile.isFile());
    Assert.assertEquals("user1", nestedFile.getOwner());
    Assert.assertEquals("group1", nestedFile.getGroup());
    Assert.assertEquals(TEST_FILE_MODE.toShort(), nestedFile.getMode());
  }

  /**
   * Tests the {@link InodeTree#createPath(LockedInodePath, CreatePathOptions)} method.
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
    List<Inode<?>> modified = createResult.getModified();
    List<Inode<?>> created = createResult.getCreated();

    // 1 modified directory
    Assert.assertEquals(1, modified.size());
    Assert.assertEquals("", modified.get(0).getName());
    Assert.assertNotEquals(lastModTime, modified.get(0).getLastModificationTimeMs());
    // 2 created directories
    Assert.assertEquals(2, created.size());
    Assert.assertEquals("nested", created.get(0).getName());
    Assert.assertEquals("test", created.get(1).getName());
    // save the last mod time of 'test'
    lastModTime = created.get(1).getLastModificationTimeMs();
    // sleep to ensure a different last modification time
    CommonUtils.sleepMs(10);

    // creating the directory path again results in no new inodes.
    try {
      createPath(mTree, NESTED_URI, dirOptions);
      Assert.assertTrue("createPath should throw FileAlreadyExistsException", false);
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(NESTED_URI));
    }

    // create a file
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    createResult = createPath(mTree, NESTED_FILE_URI, options);
    modified = createResult.getModified();
    created = createResult.getCreated();

    // test directory was modified
    Assert.assertEquals(1, modified.size());
    Assert.assertEquals("test", modified.get(0).getName());
    Assert.assertNotEquals(lastModTime, modified.get(0).getLastModificationTimeMs());
    // file was created
    Assert.assertEquals(1, created.size());
    Assert.assertEquals("file", created.get(0).getName());
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
    Assert.assertTrue(mTree.inodeIdExists(0));
    Assert.assertFalse(mTree.inodeIdExists(1));

    createPath(mTree, TEST_URI, sFileOptions);
    Inode<?> inode = getInodeByPath(mTree, TEST_URI);
    Assert.assertTrue(mTree.inodeIdExists(inode.getId()));

    deleteInodeByPath(mTree, TEST_URI);
    Assert.assertFalse(mTree.inodeIdExists(inode.getId()));
  }

  /**
   * Tests {@link InodeTree#inodePathExists(AlluxioURI)}.
   */
  @Test
  public void inodePathExists() throws Exception {
    Assert.assertFalse(mTree.inodePathExists(TEST_URI));

    createPath(mTree, TEST_URI, sFileOptions);
    Assert.assertTrue(mTree.inodePathExists(TEST_URI));

    deleteInodeByPath(mTree, TEST_URI);
    Assert.assertFalse(mTree.inodePathExists(TEST_URI));
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing path.
   */
  @Test
  public void getInodeByNonexistingPath() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Path /test does not exist");

    Assert.assertFalse(mTree.inodePathExists(TEST_URI));
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
    Assert.assertFalse(mTree.inodePathExists(NESTED_FILE_URI));
    getInodeByPath(mTree, NESTED_FILE_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode with an invalid id.
   */
  @Test
  public void getInodeByInvalidId() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(1));

    Assert.assertFalse(mTree.inodeIdExists(1));
    try (LockedInodePath inodePath = mTree.lockFullInodePath(1, InodeTree.LockMode.READ)) {
      // inode exists
    }
  }

  /**
   * Tests the {@link InodeTree#isRootId(long)} method.
   */
  @Test
  public void isRootId() {
    Assert.assertTrue(mTree.isRootId(0));
    Assert.assertFalse(mTree.isRootId(1));
  }

  /**
   * Tests the {@link InodeTree#getPath(Inode)} method.
   */
  @Test
  public void getPath() throws Exception {
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.READ)) {
      Inode<?> root = inodePath.getInode();
      // test root path
      Assert.assertEquals(new AlluxioURI("/"), mTree.getPath(root));
    }

    // test one level
    createPath(mTree, TEST_URI, sDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(TEST_URI, InodeTree.LockMode.READ)) {
      Assert.assertEquals(new AlluxioURI("/test"), mTree.getPath(inodePath.getInode()));
    }

    // test nesting
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.READ)) {
      Assert.assertEquals(new AlluxioURI("/nested/test"), mTree.getPath(inodePath.getInode()));
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
        InodeLockList lockList = mTree.lockDescendants(inodePath, InodeTree.LockMode.READ)) {
      List<Inode<?>> inodes = lockList.getInodes();
      // /test, /nested, /nested/test, /nested/test/file
      Assert.assertEquals(4, inodes.size());
    }
  }

  /**
   * Tests deleting a nested inode.
   */
  @Test
  public void deleteInode() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.WRITE)) {
      try (InodeLockList lockList = mTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodes = lockList.getInodes();
        // /nested, /nested/test
        Assert.assertEquals(2, inodes.size());
      }
      // delete the nested inode
      deleteInodeByPath(mTree, NESTED_URI);

      try (InodeLockList lockList = mTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodes = lockList.getInodes();
        // only /nested left
        Assert.assertEquals(1, inodes.size());
      }
    }
  }

  /**
   * Tests the {@link InodeTree#setPinned(LockedInodePath, boolean)} method.
   */
  @Test
  public void setPinned() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    // no inodes pinned
    Assert.assertEquals(0, mTree.getPinIdSet().size());

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.WRITE)) {
      mTree.setPinned(inodePath, true);
    }
    // nested file pinned
    Assert.assertEquals(1, mTree.getPinIdSet().size());

    // unpin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, InodeTree.LockMode.WRITE)) {
      mTree.setPinned(inodePath, false);
    }
    Assert.assertEquals(0, mTree.getPinIdSet().size());
  }

  /**
   * Tests that streaming to a journal checkpoint works.
   */
  @Test
  public void streamToJournalCheckpoint() throws Exception {
    InodeDirectory root = mTree.getRoot();

    // test root
    verifyJournal(mTree, Lists.<Inode<?>>newArrayList(root));

    // test nested URI
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    Inode<?> file = test.getChild("file");
    verifyJournal(mTree, Arrays.asList(root, nested, test, file));

    // add a sibling of test and verify journaling is in correct order (breadth first)
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    Inode<?> file1 = test1.getChild("file1");
    verifyJournal(mTree, Arrays.asList(root, nested, test, test1, file, file1));
  }

  /**
   * Tests the {@link InodeTree#addInodeFileFromJournal} and
   * {@link InodeTree#addInodeDirectoryFromJournal} methods.
   */
  @Test
  public void addInodeFromJournal() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectory root = mTree.getRoot();
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    Inode<?> file = test.getChild("file");
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    Inode<?> file1 = test1.getChild("file1");

    // reset the tree
    mTree.addInodeDirectoryFromJournal(root.toJournalEntry().getInodeDirectory());

    // re-init the root since the tree was reset above
    mTree.getRoot();

    try (LockedInodePath inodePath =
        mTree.lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ)) {
      try (InodeLockList lockList = mTree.lockDescendants(inodePath, InodeTree.LockMode.READ)) {
        Assert.assertEquals(0, lockList.getInodes().size());
      }

      mTree.addInodeDirectoryFromJournal(nested.toJournalEntry().getInodeDirectory());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested"));

      mTree.addInodeDirectoryFromJournal(test.toJournalEntry().getInodeDirectory());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test"));

      mTree.addInodeDirectoryFromJournal(test1.toJournalEntry().getInodeDirectory());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1"));

      mTree.addInodeFileFromJournal(file.toJournalEntry().getInodeFile());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1", "file"));

      mTree.addInodeFileFromJournal(file1.toJournalEntry().getInodeFile());
      verifyChildrenNames(mTree, inodePath,
          Sets.newHashSet("nested", "test", "test1", "file", "file1"));
    }
  }

  @Test
  public void getInodePathById() throws Exception {
    try (LockedInodePath rootPath = mTree.lockFullInodePath(0, InodeTree.LockMode.READ)) {
      Assert.assertEquals(0, rootPath.getInode().getId());
    }

    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    for (Inode<?> inode : createResult.getCreated()) {
      long id = inode.getId();
      try (LockedInodePath inodePath = mTree.lockFullInodePath(id, InodeTree.LockMode.READ)) {
        Assert.assertEquals(id, inodePath.getInode().getId());
      }
    }
  }

  @Test
  public void getInodePathByPath() throws Exception {
    try (LockedInodePath rootPath =
        mTree.lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ)) {
      Assert.assertTrue(mTree.isRootId(rootPath.getInode().getId()));
    }

    // Create a nested file.
    createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    AlluxioURI uri = new AlluxioURI("/nested");
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      Assert.assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      Assert.assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_FILE_URI;
    try (LockedInodePath inodePath = mTree.lockFullInodePath(uri, InodeTree.LockMode.READ)) {
      Assert.assertEquals(uri.getName(), inodePath.getInode().getName());
    }
  }

  @Test
  public void tempInodePathWithNoDescendant() throws Exception {
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    for (Inode<?> inode : createResult.getCreated()) {
      long id = inode.getId();
      try (LockedInodePath inodePath = mTree.lockFullInodePath(id, InodeTree.LockMode.READ)) {
        TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
        Assert.assertEquals(inodePath.getInode(), tempInodePath.getInode());
        Assert.assertEquals(inodePath.getUri(), tempInodePath.getUri());
        Assert.assertEquals(inodePath.getParentInodeDirectory(),
            tempInodePath.getParentInodeDirectory());
        Assert.assertEquals(inodePath.getInodeList(), tempInodePath.getInodeList());
        Assert.assertEquals(inodePath.fullPathExists(), tempInodePath.fullPathExists());
      }
    }
  }

  @Test
  public void tempInodePathWithDirectDescendant() throws Exception {
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    Inode<?> parentInode = createResult.getCreated().get(0);
    Assert.assertTrue(parentInode.isDirectory());
    Inode<?> childInode = createResult.getCreated().get(1);
    Assert.assertTrue(childInode.isDirectory());
    long parentId = parentInode.getId();
    long childId = childInode.getId();
    AlluxioURI childUri;
    List<Inode<?>> childInodeList;
    try (LockedInodePath lockedChildPath =
        mTree.lockFullInodePath(childId, InodeTree.LockMode.READ)) {
      childUri = lockedChildPath.getUri();
      childInodeList = lockedChildPath.getInodeList();
    }
    try (LockedInodePath locked = mTree.lockFullInodePath(parentId, InodeTree.LockMode.READ)) {
      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(locked);
      tempInodePath.setDescendant(childInode, childUri);
      Assert.assertEquals(childInode, tempInodePath.getInode());
      Assert.assertEquals(childUri, tempInodePath.getUri());
      Assert.assertEquals(true, tempInodePath.fullPathExists());
      // Get inode list of the direct ancestor is support.
      Assert.assertEquals(childInodeList, tempInodePath.getInodeList());
      // Get parent inode directory of the direct ancestor is support.
      Assert.assertEquals(parentInode, tempInodePath.getParentInodeDirectory());
    }
  }

  @Test
  public void tempInodePathWithIndirectDescendant() throws Exception {
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_FILE_URI, sNestedFileOptions);

    Inode<?> dirInode = createResult.getCreated().get(0);
    Assert.assertTrue(dirInode.isDirectory());
    int size = createResult.getCreated().size();
    Assert.assertTrue(size > 2);
    Inode<?> fileInode = createResult.getCreated().get(size - 1);
    Assert.assertTrue(fileInode.isFile());
    long dirId = dirInode.getId();
    long fileId = fileInode.getId();
    AlluxioURI fileUri;
    List<Inode<?>> fileInodeList;
    try (
        LockedInodePath lockedFilePath = mTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      fileUri = lockedFilePath.getUri();
      fileInodeList = lockedFilePath.getInodeList();
    }
    try (LockedInodePath locked = mTree.lockFullInodePath(dirId, InodeTree.LockMode.READ)) {
      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(locked);
      tempInodePath.setDescendant(fileInode, fileUri);
      Assert.assertEquals(fileInode, tempInodePath.getInode());
      Assert.assertEquals(fileUri, tempInodePath.getUri());
      Assert.assertEquals(true, tempInodePath.fullPathExists());
      try {
        // Get inode list of the indirect ancestor is not support.
        Assert.assertEquals(fileInodeList, tempInodePath.getInodeList());
        Assert.fail();
      } catch (UnsupportedOperationException e) {
        // expected
      }
      try {
        // Get parent inode directory of the indirect ancestor is not support.
        tempInodePath.getParentInodeDirectory();
        Assert.fail();
      } catch (UnsupportedOperationException e) {
        // expected
      }
    }
  }

  // Helper to create a path.
  InodeTree.CreatePathResult createPath(InodeTree root, AlluxioURI path,
      CreatePathOptions<?> options) throws FileAlreadyExistsException, BlockInfoException,
      InvalidPathException, IOException, FileDoesNotExistException {
    try (LockedInodePath inodePath = root.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      return root.createPath(inodePath, options, new NoopJournalContext());
    }
  }

  // Helper to get an inode by path. The inode is unlocked before returning.
  private static Inode<?> getInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = root.lockFullInodePath(path, InodeTree.LockMode.READ)) {
      return inodePath.getInode();
    }
  }

  // Helper to delete an inode by path.
  private static void deleteInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath = root.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      root.deleteInode(inodePath, System.currentTimeMillis(), DeleteOptions.defaults(),
          NoopJournalContext.INSTANCE);
    }
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<Inode<?>> journaled) throws Exception {
    Iterator<alluxio.proto.journal.Journal.JournalEntry> it = root.getJournalEntryIterator();
    for (Inode<?> node : journaled) {
      Assert.assertTrue(it.hasNext());
      Assert.assertEquals(node.toJournalEntry(), it.next());
    }
    Assert.assertTrue(!it.hasNext());
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, LockedInodePath inodePath,
      Set<String> childNames) throws Exception {
    try (InodeLockList lockList = tree.lockDescendants(inodePath, InodeTree.LockMode.READ)) {
      List<Inode<?>> children = lockList.getInodes();
      Assert.assertEquals(childNames.size(), children.size());
      for (Inode<?> child : children) {
        Assert.assertTrue(childNames.contains(child.getName()));
      }
    }
  }
}
