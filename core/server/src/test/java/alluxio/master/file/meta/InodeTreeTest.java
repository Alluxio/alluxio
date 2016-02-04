/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.file.meta;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.powermock.reflect.Whitebox;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.options.CreatePathOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.master.file.PermissionChecker;
import alluxio.security.authorization.PermissionStatus;
import alluxio.util.CommonUtils;

/**
 * Unit tests for {@link InodeTree}.
 */
public final class InodeTreeTest {
  private static final String TEST_PATH = "test";
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final PermissionStatus TEST_PERMISSION_STATUS =
      new PermissionStatus("user1", "", (short)0755);
  private static CreatePathOptions sFileOptions;
  private static CreatePathOptions sDirectoryOptions;
  private static CreatePathOptions sNestedFileOptions;
  private static CreatePathOptions sNestedDirectoryOptions;
  private InodeTree mTree;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the test fails
   */
  @Before
  public void before() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());

    BlockMaster blockMaster = new BlockMaster(blockJournal);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    MountTable mountTable = new MountTable();
    mTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    blockMaster.start(true);

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup");
    MasterContext.reset(conf);
    mTree.initializeRoot(TEST_PERMISSION_STATUS);
    verifyPermissionChecker(true, TEST_PERMISSION_STATUS.getUserName(), "test-supergroup");
  }

  /**
   * Sets up dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() {
    sFileOptions =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setPermissionStatus(TEST_PERMISSION_STATUS).build();
    sDirectoryOptions =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setDirectory(true).setPermissionStatus(TEST_PERMISSION_STATUS).build();
    sNestedFileOptions =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setPermissionStatus(TEST_PERMISSION_STATUS).setRecursive(true).build();
    sNestedDirectoryOptions =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setPermissionStatus(TEST_PERMISSION_STATUS).setDirectory(true).setRecursive(true)
            .build();
  }

  /**
   * Tests that initializing the root twice results in the same root.
   *
   * @throws Exception if getting the Inode by path fails
   */
  @Test
  public void initializeRootTwiceTest() throws Exception {
    Inode root = mTree.getInodeByPath(new AlluxioURI("/"));
    // initializeRoot call does nothing
    mTree.initializeRoot(TEST_PERMISSION_STATUS);
    verifyPermissionChecker(true, root.getUserName(), "test-supergroup");
    Inode newRoot = mTree.getInodeByPath(new AlluxioURI("/"));
    Assert.assertEquals(root, newRoot);
  }

  /**
   * Tests the {@link InodeTree#createPath(AlluxioURI, CreatePathOptions)} method for creating
   * directories.
   *
   * @throws Exception if creating the directory fails
   */
  @Test
  public void createDirectoryTest() throws Exception {
    // create directory
    mTree.createPath(TEST_URI, sDirectoryOptions);
    Inode test = mTree.getInodeByPath(TEST_URI);
    Assert.assertEquals(TEST_PATH, test.getName());
    Assert.assertTrue(test.isDirectory());
    Assert.assertEquals("user1", test.getUserName());
    Assert.assertTrue(test.getGroupName().isEmpty());
    Assert.assertEquals((short)0755, test.getPermission());

    // create nested directory
    mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    Inode nested = mTree.getInodeByPath(NESTED_URI);
    Assert.assertEquals(TEST_PATH, nested.getName());
    Assert.assertEquals(2, nested.getParentId());
    Assert.assertTrue(test.isDirectory());
    Assert.assertEquals("user1", test.getUserName());
    Assert.assertTrue(test.getGroupName().isEmpty());
    Assert.assertEquals((short)0755, test.getPermission());
  }

  /**
   * Tests that an exception is thrown when trying to create an already existing directory with the
   * {@code allowExists} flag set to {@code false}.
   *
   * @throws Exception if creating the directory fails
   */
  @Test
  public void createExistingDirectoryTest() throws Exception {
    // create directory
    mTree.createPath(TEST_URI, sDirectoryOptions);

    // create again with allowExists true
    mTree.createPath(TEST_URI, new CreatePathOptions.Builder().setAllowExists(true).build());

    // create again with allowExists false
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(TEST_URI));
    mTree.createPath(TEST_URI, new CreatePathOptions.Builder().setAllowExists(false).build());
  }

  /**
   * Tests that creating a file under a pinned directory works.
   *
   * @throws Exception if creating the directory fails
   */
  @Test
  public void createFileUnderPinnedDirectoryTest() throws Exception {
    // create nested directory
    InodeTree.CreatePathResult createResult = mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    List<Inode> created = createResult.getCreated();
    Inode nested = created.get(created.size() - 1);

    // pin nested folder
    mTree.setPinned(nested, true);

    // create nested file under pinned folder
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);

    // the nested file is pinned
    Assert.assertEquals(1, mTree.getPinIdSet().size());
  }

  /**
   * Tests the {@link InodeTree#createPath(AlluxioURI, CreatePathOptions)} method for creating a
   * file.
   *
   * @throws Exception if creating the directory fails
   */
  @Test
  public void createFileTest() throws Exception {
    // created nested file
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);
    Inode nestedFile = mTree.getInodeByPath(NESTED_FILE_URI);
    Assert.assertEquals("file", nestedFile.getName());
    Assert.assertEquals(2, nestedFile.getParentId());
    Assert.assertTrue(nestedFile.isFile());
    Assert.assertEquals("user1", nestedFile.getUserName());
    Assert.assertTrue(nestedFile.getGroupName().isEmpty());
    Assert.assertEquals((short)0644, nestedFile.getPermission());
  }

  /**
   * Tests the {@link InodeTree#createPath(AlluxioURI, CreatePathOptions)} method.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createPathTest() throws Exception {
    // save the last mod time of the root
    long lastModTime = mTree.getRoot().getLastModificationTimeMs();
    // sleep to ensure a different last modification time
    CommonUtils.sleepMs(10);

    // create nested directory
    InodeTree.CreatePathResult createResult =
        mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    List<Inode> modified = createResult.getModified();
    List<Inode> created = createResult.getCreated();
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
      createResult = mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
      Assert.assertTrue("createPath should throw FileAlreadyExistsException", false);
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(NESTED_URI));
    }

    // create a file
    CreatePathOptions options =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
    createResult = mTree.createPath(NESTED_FILE_URI, options);
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
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createRootPathTest() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/");

    mTree.createPath(new AlluxioURI("/"), sFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with invalid block size.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createFileWithInvalidBlockSizeTest() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size 0");

    CreatePathOptions options =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(0).build();
    mTree.createPath(TEST_URI, options);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with a negative block size.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createFileWithNegativeBlockSizeTest() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size -1");

    CreatePathOptions options =
        new CreatePathOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(-1).build();
    mTree.createPath(TEST_URI, options);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a non-existing directory.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createFileUnderNonexistingDirTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("File /nested/test creation failed. Component 1(nested) does not exist");

    mTree.createPath(NESTED_URI, sFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file twice.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createFileTwiceTest() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/nested/test");

    mTree.createPath(NESTED_URI, sNestedFileOptions);
    mTree.createPath(NESTED_URI, sNestedFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a file path.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void createFileUnderFileTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not traverse to parent directory of path /nested/test/test."
        + " Component test is not a directory.");

    mTree.createPath(NESTED_URI, sNestedFileOptions);
    mTree.createPath(new AlluxioURI("/nested/test/test"), sNestedFileOptions);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing path.
   *
   * @throws Exception if getting the Inode by path fails
   */
  @Test
  public void getInodeByNonexistingPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Path /test does not exist");

    mTree.getInodeByPath(TEST_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode by a non-existing, nested path.
   *
   * @throws Exception if creating the path or getting the Inode by path fails
   */
  @Test
  public void getInodeByNonexistingNestedPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Path /nested/test/file does not exist");

    mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    mTree.getInodeByPath(NESTED_FILE_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get an Inode with an invalid id.
   *
   * @throws Exception if getting the Inode by its id fails
   */
  @Test
  public void getInodeByInvalidIdTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Inode id 1 does not exist");

    mTree.getInodeById(1);
  }

  /**
   * Tests the {@link InodeTree#isRootId(long)} method.
   */
  @Test
  public void isRootIdTest() {
    Assert.assertTrue(mTree.isRootId(0));
    Assert.assertFalse(mTree.isRootId(1));
  }

  /**
   * Tests the {@link InodeTree#getPath(Inode)} method.
   *
   * @throws Exception if creating the path or getting the Inode by its id fails
   */
  @Test
  public void getPathTest() throws Exception {
    Inode root = mTree.getInodeById(0);
    // test root path
    Assert.assertEquals(new AlluxioURI("/"), mTree.getPath(root));

    // test one level
    InodeTree.CreatePathResult createResult = mTree.createPath(TEST_URI, sDirectoryOptions);
    List<Inode> created = createResult.getCreated();
    Assert.assertEquals(new AlluxioURI("/test"), mTree.getPath(created.get(created.size() - 1)));

    // test nesting
    createResult = mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    created = createResult.getCreated();
    Assert.assertEquals(new AlluxioURI("/nested/test"),
        mTree.getPath(created.get(created.size() - 1)));
  }

  /**
   * Tests the {@link InodeTree#getInodeChildrenRecursive(InodeDirectory)} method.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void getInodeChildrenRecursiveTest() throws Exception {
    mTree.createPath(TEST_URI, sDirectoryOptions);
    mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    // add nested file
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);

    // all inodes under root
    List<Inode> inodes = mTree.getInodeChildrenRecursive((InodeDirectory) mTree.getInodeById(0));
    // /test, /nested, /nested/test, /nested/test/file
    Assert.assertEquals(4, inodes.size());
  }

  /**
   * Tests the {@link InodeTree#deleteInode(Inode)} method.
   *
   * @throws Exception if an {@link InodeTree} operation fails
   */
  @Test
  public void deleteInodeTest() throws Exception {
    InodeTree.CreatePathResult createResult =
        mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    List<Inode> created = createResult.getCreated();

    // all inodes under root
    List<Inode> inodes = mTree.getInodeChildrenRecursive((InodeDirectory) mTree.getInodeById(0));
    // /nested, /nested/test
    Assert.assertEquals(2, inodes.size());
    // delete the nested inode
    mTree.deleteInode(created.get(created.size() - 1));
    inodes = mTree.getInodeChildrenRecursive((InodeDirectory) mTree.getInodeById(0));
    // only /nested left
    Assert.assertEquals(1, inodes.size());
  }

  /**
   * Tests that an exception is thrown when trying to delete a non-existing Inode.
   *
   * @throws Exception if deleting the Inode fails
   */
  @Test
  public void deleteNonexistingInodeTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Inode id 1 does not exist");

    Inode testFile = new InodeFile.Builder().setName("testFile1").setId(1).setParentId(1)
        .setPermissionStatus(TEST_PERMISSION_STATUS).build();
    mTree.deleteInode(testFile);
  }

  /**
   * Tests the {@link InodeTree#setPinned(Inode, boolean)} method.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void setPinnedTest() throws Exception {
    InodeTree.CreatePathResult createResult =
        mTree.createPath(NESTED_URI, sNestedDirectoryOptions);
    List<Inode> created = createResult.getCreated();
    Inode nested = created.get(created.size() - 1);
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);

    // no inodes pinned
    Assert.assertEquals(0, mTree.getPinIdSet().size());

    // pin nested folder
    mTree.setPinned(nested, true);
    // nested file pinned
    Assert.assertEquals(1, mTree.getPinIdSet().size());

    // unpin nested folder
    mTree.setPinned(nested, false);
    Assert.assertEquals(0, mTree.getPinIdSet().size());
  }

  /**
   * Tests that streaming to a journal checkpoint works.
   *
   * @throws Exception if creating the path fails
   */
  @Test
  public void streamToJournalCheckpointTest() throws Exception {
    InodeDirectory root = mTree.getRoot();

    // test root
    verifyJournal(mTree, Lists.<Inode>newArrayList(root));

    // test nested URI
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    Inode file = test.getChild("file");
    verifyJournal(mTree, Lists.newArrayList(root, nested, test, file));

    // add a sibling of test and verify journaling is in correct order (breadth first)
    mTree.createPath(new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    Inode file1 = test1.getChild("file1");
    verifyJournal(mTree, Lists.newArrayList(root, nested, test, test1, file, file1));
  }

  /**
   * Tests the {@link InodeTree#addInodeFromJournal(alluxio.proto.journal.Journal.JournalEntry)}
   * method.
   *
   * @throws Exception if creating a path fails
   */
  @Test
  public void addInodeFromJournalTest() throws Exception {
    mTree.createPath(NESTED_FILE_URI, sNestedFileOptions);
    mTree.createPath(new AlluxioURI("/nested/test1/file1"), sNestedFileOptions);
    InodeDirectory root = mTree.getRoot();
    InodeDirectory nested = (InodeDirectory) root.getChild("nested");
    InodeDirectory test = (InodeDirectory) nested.getChild("test");
    Inode file = test.getChild("file");
    InodeDirectory test1 = (InodeDirectory) nested.getChild("test1");
    Inode file1 = test1.getChild("file1");

    // reset the tree
    mTree.addInodeFromJournal(root.toJournalEntry());

    // re-init the root since the tree was reset above
    root = mTree.getRoot();

    Assert.assertEquals(0, mTree.getInodeChildrenRecursive(root).size());
    mTree.addInodeFromJournal(nested.toJournalEntry());
    verifyChildrenNames(mTree, root, Sets.newHashSet("nested"));

    mTree.addInodeFromJournal(test.toJournalEntry());
    verifyChildrenNames(mTree, root, Sets.newHashSet("nested", "test"));

    mTree.addInodeFromJournal(test1.toJournalEntry());
    verifyChildrenNames(mTree, root, Sets.newHashSet("nested", "test", "test1"));

    mTree.addInodeFromJournal(file.toJournalEntry());
    verifyChildrenNames(mTree, root, Sets.newHashSet("nested", "test", "test1", "file"));

    mTree.addInodeFromJournal(file1.toJournalEntry());
    verifyChildrenNames(mTree, root, Sets.newHashSet("nested", "test", "test1", "file", "file1"));
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<Inode> journaled) throws Exception {
    JournalOutputStream mockOutputStream = Mockito.mock(JournalOutputStream.class);
    root.streamToJournalCheckpoint(mockOutputStream);
    for (Inode node : journaled) {
      Mockito.verify(mockOutputStream).writeEntry(node.toJournalEntry());
    }
    Mockito.verifyNoMoreInteractions(mockOutputStream);
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, InodeDirectory root,
      Set<String> childNames) throws Exception {
    List<Inode> children = tree.getInodeChildrenRecursive(root);
    Assert.assertEquals(childNames.size(), children.size());
    for (Inode child : children) {
      Assert.assertTrue(childNames.contains(child.getName()));
    }
  }

  private void verifyPermissionChecker(boolean enabled, String owner, String group) {
    Assert.assertEquals(enabled, Whitebox.getInternalState(PermissionChecker.class,
        "sPermissionCheckEnabled"));
    Assert.assertEquals(owner, Whitebox.getInternalState(PermissionChecker.class,
        "sFileSystemOwner"));
    Assert.assertEquals(group, Whitebox.getInternalState(PermissionChecker.class,
        "sFileSystemSuperGroup"));
  }
}
