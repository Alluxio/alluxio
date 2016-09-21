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
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authorization.Permission;
import alluxio.util.CommonUtils;

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
import java.util.Arrays;
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
  private static final Permission TEST_PERMISSION = new Permission("user1", "", (short) 0755);
  private static CreateFileOptions sFileOptions;
  private static CreateDirectoryOptions sDirectoryOptions;
  private static CreateFileOptions sNestedFileOptions;
  private static CreateDirectoryOptions sNestedDirectoryOptions;
  private InodeTree mTree;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());

    BlockMaster blockMaster = new BlockMaster(blockJournal);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    MountTable mountTable = new MountTable();
    mTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    blockMaster.start(true);

    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup");
    mTree.initializeRoot(TEST_PERMISSION);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Sets up dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermission(TEST_PERMISSION);
    sDirectoryOptions =
        CreateDirectoryOptions.defaults().setPermission(TEST_PERMISSION);
    sNestedFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermission(TEST_PERMISSION).setRecursive(true);
    sNestedDirectoryOptions =
        CreateDirectoryOptions.defaults().setPermission(TEST_PERMISSION).setRecursive(true);
  }

  /**
   * Tests that initializing the root twice results in the same root.
   */
  @Test
  public void initializeRootTwice() throws Exception {
    Inode<?> root = getInodeByPath(mTree, new AlluxioURI("/"));
    // initializeRoot call does nothing
    mTree.initializeRoot(TEST_PERMISSION);
    Assert.assertEquals(TEST_PERMISSION.getOwner(), root.getOwner());
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
    Assert.assertTrue(test.getGroup().isEmpty());
    Assert.assertEquals((short) 0755, test.getMode());

    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    Assert.assertTrue(mTree.inodePathExists(NESTED_URI));
    Inode<?> nested = getInodeByPath(mTree, NESTED_URI);
    Assert.assertEquals(TEST_PATH, nested.getName());
    Assert.assertEquals(2, nested.getParentId());
    Assert.assertTrue(test.isDirectory());
    Assert.assertEquals("user1", test.getOwner());
    Assert.assertTrue(test.getGroup().isEmpty());
    Assert.assertEquals((short) 0755, test.getMode());
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
   * Tests the {@link InodeTree#createPath(LockedInodePath, CreatePathOptions)} method for
   * creating a file.
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
    Assert.assertTrue(nestedFile.getGroup().isEmpty());
    Assert.assertEquals((short) 0644, nestedFile.getMode());
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

    // create nested directory
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
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
      createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
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
         InodeLockList lockList = mTree
             .lockDescendants(inodePath, InodeTree.LockMode.READ)) {
      List<Inode<?>> inodes = lockList.getInodes();
      // /test, /nested, /nested/test, /nested/test/file
      Assert.assertEquals(4, inodes.size());
    }
  }

  /**
   * Tests the {@link InodeTree#deleteInode(LockedInodePath)} method.
   */
  @Test
  public void deleteInode() throws Exception {
    InodeTree.CreatePathResult createResult =
        createPath(mTree, NESTED_URI, sNestedDirectoryOptions);
    List<Inode<?>> created = createResult.getCreated();

    // all inodes under root
    try (LockedInodePath inodePath = mTree.lockFullInodePath(0, InodeTree.LockMode.WRITE)) {
      try (InodeLockList lockList = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodes = lockList.getInodes();
        // /nested, /nested/test
        Assert.assertEquals(2, inodes.size());
      }
      // delete the nested inode
      deleteInodeByPath(mTree, NESTED_URI);

      try (InodeLockList lockList = mTree.lockDescendants(inodePath,
          InodeTree.LockMode.WRITE)) {
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
   * Tests the {@link InodeTree#addInodeFromJournal(alluxio.proto.journal.Journal.JournalEntry)}
   * method.
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
    mTree.addInodeFromJournal(root.toJournalEntry());

    // re-init the root since the tree was reset above
    root = mTree.getRoot();

    try (
        LockedInodePath inodePath = mTree
            .lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ)) {
      try (InodeLockList lockList = mTree
          .lockDescendants(inodePath, InodeTree.LockMode.READ)) {
        Assert.assertEquals(0, lockList.getInodes().size());
      }

      mTree.addInodeFromJournal(nested.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested"));

      mTree.addInodeFromJournal(test.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test"));

      mTree.addInodeFromJournal(test1.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1"));

      mTree.addInodeFromJournal(file.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1", "file"));

      mTree.addInodeFromJournal(file1.toJournalEntry());
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
    try (LockedInodePath rootPath = mTree
        .lockFullInodePath(new AlluxioURI("/"), InodeTree.LockMode.READ)) {
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

  // Helper to create a path.
  InodeTree.CreatePathResult createPath(InodeTree root, AlluxioURI path,
      CreatePathOptions<?> options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    try (LockedInodePath inodePath = root.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      return root.createPath(inodePath, options);
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
      root.deleteInode(inodePath);
    }
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<Inode<?>> journaled) throws Exception {
    JournalOutputStream mockOutputStream = Mockito.mock(JournalOutputStream.class);
    root.streamToJournalCheckpoint(mockOutputStream);
    for (Inode<?> node : journaled) {
      Mockito.verify(mockOutputStream).writeEntry(node.toJournalEntry());
    }
    Mockito.verifyNoMoreInteractions(mockOutputStream);
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, LockedInodePath inodePath,
      Set<String> childNames) throws Exception {
    try (InodeLockList lockList = tree
        .lockDescendants(inodePath, InodeTree.LockMode.READ)) {
      List<Inode<?>> children = lockList.getInodes();
      Assert.assertEquals(childNames.size(), children.size());
      for (Inode<?> child : children) {
        Assert.assertTrue(childNames.contains(child.getName()));
      }
    }
  }
}
