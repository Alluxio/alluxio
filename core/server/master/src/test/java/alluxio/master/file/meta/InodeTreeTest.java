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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.CreatePathContext;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Unit tests for {@link InodeTree}.
 */
@RunWith(Parameterized.class)
public final class InodeTreeTest {
  private static final String TEST_PATH = "test";
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_DIR_URI = new AlluxioURI("/nested/test/dir");
  private static final AlluxioURI NESTED_DIR_FILE_URI = new AlluxioURI("/nested/test/dir/file1");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final AlluxioURI NESTED_MULTIDIR_FILE_URI
      = new AlluxioURI("/nested/test/dira/dirb/file");
  public static final String TEST_OWNER = "user1";
  public static final String TEST_GROUP = "group1";
  public static final Mode TEST_DIR_MODE = new Mode((short) 0755);
  public static final Mode TEST_FILE_MODE = new Mode((short) 0644);
  private static CreateFileContext sFileContext;
  private static CreateDirectoryContext sDirectoryContext;
  private static CreateFileContext sNestedFileContext;
  private static CreateDirectoryContext sNestedDirectoryContext;
  private InodeStore mInodeStore;
  private InodeTree mTree;
  private MasterRegistry mRegistry;
  private MetricsMaster mMetricsMaster;

  @Parameters
  public static Iterable<Supplier<InodeStore>> parameters() throws Exception {
    String dir =
            AlluxioTestDirectory.createTemporaryDirectory("inode-tree-test").getAbsolutePath();
    return Arrays.asList(
        () -> new CachingInodeStore(new RocksInodeStore(dir), new InodeLockManager()),
        () -> new CachingInodeStore(new HeapInodeStore(), new InodeLockManager()),
        HeapInodeStore::new,
        () -> new RocksInodeStore(dir));
  }

  public InodeTreeTest(Supplier<InodeStore> supplier) {
    mInodeStore = supplier.get();
  }

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, Object>()
          .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true)
          .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup")
          .build(), Configuration.modifiableGlobal());

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
    mTree = new InodeTree(mInodeStore, blockMaster, directoryIdGenerator, mountTable, lockManager);

    mRegistry.start(true);

    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE, NoopJournalContext.INSTANCE);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    mInodeStore.close();
  }

  /**
   * Sets up dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileContext =
        CreateFileContext
            .mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
                .setMode(TEST_FILE_MODE.toProto()))
            .setOwner(TEST_OWNER).setGroup(TEST_GROUP);
    sDirectoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setMode(TEST_DIR_MODE.toProto()))
        .setOwner(TEST_OWNER).setGroup(TEST_GROUP);
    sNestedFileContext = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
            .setMode(TEST_FILE_MODE.toProto()).setRecursive(true))
        .setOwner(TEST_OWNER).setGroup(TEST_GROUP);
    sNestedDirectoryContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder().setMode(TEST_DIR_MODE.toProto()).setRecursive(true))
        .setOwner(TEST_OWNER).setGroup(TEST_GROUP);
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
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathContext)}
   * method for creating directories.
   */
  @Test
  public void createDirectory() throws Exception {
    // create directory
    createPath(mTree, TEST_URI, sDirectoryContext);
    assertTrue(mTree.inodePathExists(TEST_URI));
    MutableInode<?> test = getInodeByPath(TEST_URI);
    assertEquals(TEST_PATH, test.getName());
    assertTrue(test.isDirectory());
    assertEquals("user1", test.getOwner());
    assertEquals("group1", test.getGroup());
    assertEquals(TEST_DIR_MODE.toShort(), test.getMode());

    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
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
    createPath(mTree, TEST_URI, sDirectoryContext);

    // create again with allowExists true
    createPath(mTree, TEST_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setAllowExists(true)));

    // create again with allowExists false
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown
        .expectMessage("Not allowed to create directory because path already exists: " + TEST_URI);
    createPath(mTree, TEST_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setAllowExists(false)));
  }

  /**
   * Tests that creating a file under a pinned directory works.
   */
  @Test
  public void createFileUnderPinnedDirectory() throws Exception {
    // create nested directory
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(
            NESTED_URI, LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)
    ) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true, Collections.emptyList(), 0);
    }

    // create nested file under pinned folder
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    // the nested file is pinned
    assertEquals(1, mTree.getPinIdSet().size());
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathContext)}
   * method for creating a file.
   */
  @Test
  public void createFile() throws Exception {
    // created nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);
    MutableInode<?> nestedFile = getInodeByPath(NESTED_FILE_URI);
    assertEquals("file", nestedFile.getName());
    assertEquals(2, nestedFile.getParentId());
    assertTrue(nestedFile.isFile());
    assertEquals("user1", nestedFile.getOwner());
    assertEquals("group1", nestedFile.getGroup());
    assertEquals(TEST_FILE_MODE.toShort(), nestedFile.getMode());
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathContext)}
   * method.
   */
  @Test
  public void createPathTest() throws Exception {
    // save the last mod time of the root
    long lastModTime = mTree.getRoot().getLastModificationTimeMs();
    // sleep to ensure a different last modification time
    CommonUtils.sleepMs(10);

    // Need to use updated options to set the correct last mod time.
    CreateDirectoryContext dirContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setMode(TEST_DIR_MODE.toProto()))
            .setOwner(TEST_OWNER).setGroup(TEST_GROUP);

    // create nested directory
    List<Inode> created = createPath(mTree, NESTED_URI, dirContext);
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
      createPath(mTree, NESTED_URI, dirContext);
      fail("createPath should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      assertEquals("Not allowed to create directory because path already exists: " + NESTED_URI,
          e.getMessage());
    }

    // create a file
    CreateFileContext options = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    created = createPath(mTree, NESTED_FILE_URI, options);

    // test directory was modified
    assertNotEquals(lastModTime, getInodeByPath(NESTED_URI).getLastModificationTimeMs());
    // file was created
    assertEquals(1, created.size());
    assertEquals("file", created.get(0).getName());

    // creating the file path again results in no new inodes.
    try {
      createPath(mTree, NESTED_FILE_URI, options);
      fail("createPath should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      assertEquals("Not allowed to create file because path already exists: " + NESTED_FILE_URI,
          e.getMessage());
    }
  }

  /**
   * Tests the {@link InodeTree#createPath(RpcContext, LockedInodePath, CreatePathContext)} method
   * for inheriting owner and group when empty.
   */
  @Test
  public void createPathInheritanceTest() throws Exception {
    // create nested directory
    CreateDirectoryContext dirContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setMode(TEST_DIR_MODE.toProto()))
            .setOwner(TEST_OWNER).setGroup(TEST_GROUP);
    List<Inode> created = createPath(mTree, NESTED_URI, dirContext);
    assertEquals(2, created.size());

    // 1. create a nested directory with empty owner and group
    CreateDirectoryContext nestedDirContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setMode(TEST_DIR_MODE.toProto()))
        .setOwner("").setGroup("");
    created = createPath(mTree, NESTED_DIR_URI, nestedDirContext);
    assertEquals(1, created.size());
    assertEquals("dir", created.get(0).getName());
    assertEquals(TEST_OWNER, created.get(0).getOwner());
    assertEquals(TEST_GROUP, created.get(0).getGroup());

    // 2. create a file with empty owner and group
    CreateFileContext nestedDirFileContext = CreateFileContext
        .mergeFrom(
            CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true))
        .setOwner("").setGroup("");
    created = createPath(mTree, NESTED_DIR_FILE_URI, nestedDirFileContext);
    assertEquals(1, created.size());
    assertEquals("file1", created.get(0).getName());
    assertEquals(TEST_OWNER, created.get(0).getOwner());
    assertEquals(TEST_GROUP, created.get(0).getGroup());
  }

  /**
   * Tests that an exception is thrown when trying to create the root path twice.
   */
  @Test
  public void createRootPath() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/");

    createPath(mTree, new AlluxioURI("/"), sFileContext);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with invalid block size.
   */
  @Test
  public void createFileWithInvalidBlockSize() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size 0");

    CreateFileContext context =
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(0));
    createPath(mTree, TEST_URI, context);
  }

  /**
   * Tests that an exception is thrown when trying to create a file with a negative block size.
   */
  @Test
  public void createFileWithNegativeBlockSize() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size -1");

    CreateFileContext context =
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(-1));
    createPath(mTree, TEST_URI, context);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a non-existing directory.
   */
  @Test
  public void createFileUnderNonexistingDir() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("File /nested/test creation failed. Component 1(nested) does not exist");

    createPath(mTree, NESTED_URI, sFileContext);
  }

  /**
   * Tests that an exception is thrown when trying to create a file twice.
   */
  @Test
  public void createFileTwice() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mThrown.expectMessage("/nested/test");

    createPath(mTree, NESTED_URI, sNestedFileContext);
    createPath(mTree, NESTED_URI, sNestedFileContext);
  }

  /**
   * Tests that an exception is thrown when trying to create a file under a file path.
   */
  @Test
  public void createFileUnderFile() throws Exception {
    createPath(mTree, NESTED_URI, sNestedFileContext);

    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed for path /nested/test/test. "
        + "Component 2(test) is a file, not a directory");
    createPath(mTree, new AlluxioURI("/nested/test/test"), sNestedFileContext);
  }

  /**
   * Tests {@link InodeTree#inodeIdExists(long)}.
   */
  @Test
  public void inodeIdExists() throws Exception {
    assertTrue(mTree.inodeIdExists(0));
    assertFalse(mTree.inodeIdExists(1));

    createPath(mTree, TEST_URI, sFileContext);
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

    createPath(mTree, TEST_URI, sFileContext);
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
   * Tests that an exception is thrown when trying to get an Inode by a non-existing, nested
   * path.
   */
  @Test
  public void getInodeByNonexistingNestedPath() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Path \"/nested/test/file\" does not exist");

    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
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
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(1, LockPattern.READ, NoopJournalContext.INSTANCE)) {
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
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(0, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      Inode root = inodePath.getInode();
      // test root path
      assertEquals(new AlluxioURI("/"), mTree.getPath(root));
    }

    // test one level
    createPath(mTree, TEST_URI, sDirectoryContext);
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(TEST_URI, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      assertEquals(new AlluxioURI("/test"), mTree.getPath(inodePath.getInode()));
    }

    // test nesting
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(
        NESTED_URI, LockPattern.READ, NoopJournalContext.INSTANCE)
    ) {
      assertEquals(new AlluxioURI("/nested/test"), mTree.getPath(inodePath.getInode()));
    }
  }

  List<AlluxioURI> setupBaseFiles(int fileCount, String parent) throws Exception {
    List<AlluxioURI> files = new ArrayList<>();
    for (int i = 0; i < fileCount; i++) {
      files.add(new AlluxioURI(String.format("%s/%d", parent, i)));
    }

    // insert in reverse order so the inodes are created not in sorted order
    for (int i = files.size() - 1; i >= 0; i--) {
      createPath(mTree, files.get(i), sNestedFileContext);
    }
    return files;
  }

  public void checkChildren(List<AlluxioURI> files) throws Exception {
    int fileCount = files.size();
    int remain = fileCount;
    for (AlluxioURI nxt : files) {
      try (LockedInodePath inodePath = mTree.lockFullInodePath(nxt, LockPattern.READ)) {
        long parentId = inodePath.getInode().getParentId();
        Inode childInode = inodePath.getInode();
        Inode[] childInodes;
        try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenFrom(parentId,
            childInode.getName())) {
          childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
        }
        assertEquals(remain, childInodes.length);
        ArrayList<AlluxioURI> remainFiles = files.stream().skip(fileCount - remain)
            .collect(Collectors.toCollection(ArrayList::new));
        for (Inode inode : childInodes) {
          AlluxioURI path = mTree.getPath(inode.getId());
          assertTrue(remainFiles.contains(path));
        }
      }
      remain--;
    }
  }

  @Test
  public void getChildrenPrefix() throws Exception {
    // Test a single file prefix
    String prefix = "afile";
    AlluxioURI file = new AlluxioURI("/" + prefix);
    createPath(mTree, file, sNestedDirectoryContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(file, LockPattern.READ)) {
      long parentId = inodePath.getInode().getParentId();
      Inode[] childInodes;
      try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenPrefix(parentId,
          prefix)) {
        childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
      }
      assertEquals(1, childInodes.length);
      assertEquals(file, mTree.getPath(childInodes[0].getId()));
    }

    // Test multiple files with the prefix
    prefix = "bfile";
    List<AlluxioURI> files = new ArrayList<>(Arrays.asList(new AlluxioURI("/" + prefix),
        new AlluxioURI("/" + prefix + "1"),
        new AlluxioURI("/" + prefix + "2")));
    // add the files out of order
    for (int i = files.size() - 1; i >= 0; i--) {
      createPath(mTree, files.get(i), sNestedFileContext);
    }
    // add a nested directory that matches the prefix
    String nestedPath = "/" + prefix + "3";
    createPath(mTree, new AlluxioURI(nestedPath + "/nested"), sNestedDirectoryContext);
    files.add(new AlluxioURI(nestedPath));
    // add a file that does not match the prefix
    createPath(mTree, new AlluxioURI("/cfile"), sNestedFileContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(file, LockPattern.READ)) {
      long parentId = inodePath.getInode().getParentId();
      Inode[] childInodes;
      try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenPrefix(parentId,
          prefix)) {
        childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
      }
      assertEquals(files.size(), childInodes.length);
      for (int i = 0; i < files.size(); i++) {
        assertEquals(files.get(i), mTree.getPath(childInodes[i].getId()));
      }
    }
  }

  @Test
  public void getChildrenPrefixAfter() throws Exception {
    // Test a single file prefix
    String prefix = "afile";
    AlluxioURI file = new AlluxioURI("/" + prefix);
    createPath(mTree, file, sNestedDirectoryContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(file, LockPattern.READ)) {
      long parentId = inodePath.getInode().getParentId();
      Inode[] childInodes;
      try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenPrefixFrom(parentId,
          prefix, "afile")) {
        childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
      }
      assertEquals(1, childInodes.length);
      assertEquals(file, mTree.getPath(childInodes[0].getId()));
    }

    // Test multiple files with the prefix
    prefix = "bfile";
    List<AlluxioURI> files = new ArrayList<>(Arrays.asList(new AlluxioURI("/" + prefix),
        new AlluxioURI("/" + prefix + "1"),
        new AlluxioURI("/" + prefix + "2")));
    // add the files out of order
    for (int i = files.size() - 1; i >= 0; i--) {
      createPath(mTree, files.get(i), sNestedFileContext);
    }
    // add a nested directory that matches the prefix
    String nestedPath = "/" + prefix + "3";
    createPath(mTree, new AlluxioURI(nestedPath + "/nested"), sNestedDirectoryContext);
    files.add(new AlluxioURI(nestedPath));
    // add a file that does not match the prefix
    createPath(mTree, new AlluxioURI("/cfile"), sNestedFileContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(file, LockPattern.READ)) {
      long parentId = inodePath.getInode().getParentId();
      Inode[] childInodes;
      // read from "bfile1" with prefix
      try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenPrefixFrom(parentId,
          prefix, prefix + "1")) {
        childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
      }
      assertEquals(files.size() - 1, childInodes.length);
      for (int i = 1; i < files.size(); i++) {
        assertEquals(files.get(i), mTree.getPath(childInodes[i - 1].getId()));
      }
    }
  }

  @Test
  public void getChildrenAfterId() throws Exception {
    int fileCount = 10;
    String parent = "/nxt";
    List<AlluxioURI> files = setupBaseFiles(fileCount, parent);
    checkChildren(files);
  }

  <T> Stream<T> iteratorToStream(Iterator<T> iter) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
        iter, Spliterator.ORDERED), false);
  }

  @Test
  public void getChildAfterIdDeleted() throws Exception {
    int fileCount = 10;
    String parent = "/nxt";
    List<AlluxioURI> files = setupBaseFiles(fileCount, parent);

    // delete the file in the middle
    int middle = fileCount / 2;
    AlluxioURI toRemove = files.remove(middle);
    deleteInodeByPath(mTree, toRemove);
    checkChildren(files);
  }

  @Test
  public void getChildrenAfterIdNested() throws Exception {
    int fileCount = 10;
    int nestDepth = 10;
    String parent = "";
    List<AlluxioURI> files = new ArrayList<>();
    for (int i = 0; i < nestDepth; i++) {
      for (int j = 0; j < fileCount; j++) {
        files.add(new AlluxioURI(String.format("%s/%d", parent, j)));
      }
      parent += "/nxt";
    }

    for (AlluxioURI nxt : files) {
      createPath(mTree, nxt, sNestedFileContext);
    }

    // Go through each file and be sure there are the proper inodes following it in the listing
    parent = "";
    for (int i = 0; i < nestDepth; i++) {
      for (int j = 0; j < fileCount; j++) {
        // fileCount plus 1 for remaining nested directory (except on the bottom depth)
        int remain = fileCount - j;
        boolean notAtBottom = i < nestDepth - 1;
        if (notAtBottom) {
          remain++;
        }
        AlluxioURI nxt = new AlluxioURI(String.format("%s/%d", parent, j));
        try (LockedInodePath inodePath = mTree.lockFullInodePath(nxt, LockPattern.READ)) {
          long parentId = inodePath.getInode().getParentId();
          Inode childInode = inodePath.getInode();
          Inode[] childInodes;
          try (CloseableIterator<? extends Inode> fromIter = mInodeStore.getChildrenFrom(parentId,
              childInode.getName())) {
            childInodes = iteratorToStream(fromIter).toArray(Inode[]::new);
          }
          for (int k = 0; k < remain; k++) {
            if (notAtBottom && k == remain - 1) {
              // the last entry should be the remaining directory
              assertEquals(new AlluxioURI(String.format("%s/nxt", parent)),
                  mTree.getPath(childInodes[k].getId()));
            } else {
              assertEquals(new AlluxioURI(String.format("%s/%d", parent, k + j)),
                  mTree.getPath(childInodes[k].getId()));
            }
          }
          assertEquals(remain, childInodes.length);
        }
      }
      parent += "/nxt";
    }
  }

  @Test
  public void getPathById() throws Exception {
    // test nesting
    long id;
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.READ)) {
      assertEquals(NESTED_URI, mTree.getPath(inodePath.getInode()));
      id = inodePath.getInode().getId();
    }
    assertEquals(NESTED_URI, mTree.getPath(id));

    // test path not exists
    assertThrows(FileDoesNotExistException.class, () -> mTree.getPath(id + 1));
  }

  @Test
  public void getInodesById() throws Exception {
    // test nesting
    long id;
    ArrayList<String> pathIds;
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
    try (LockedInodePath inodePath = mTree.lockFullInodePath(NESTED_URI, LockPattern.READ)) {
      pathIds = inodePath.getInodeList().stream().map(Inode::getName).collect(
          Collectors.toCollection(ArrayList::new));
      assertEquals(pathIds, mTree.getPathInodeNames(inodePath.getInode()));
      id = inodePath.getInode().getId();
    }
    assertEquals(pathIds, mTree.getPathInodeNames(id));

    // test path not exists
    assertThrows(FileDoesNotExistException.class, () -> mTree.getPathInodeNames(id + 1));
  }

  @Test
  public void getInodeChildrenRecursive() throws Exception {
    createPath(mTree, TEST_URI, sDirectoryContext);
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
    // add nested file
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    // all inodes under root
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(0, LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)) {
      // /test, /nested, /nested/test, /nested/test/file
      assertEquals(4, mTree.getDescendants(inodePath).getInodePathList().size());
    }
  }

  /**
   * Tests deleting a nested inode.
   */
  @Test
  public void deleteInode() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);

    // all inodes under root
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(0, LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)) {
      // /nested, /nested/test
      assertEquals(2, mTree.getDescendants(inodePath).getInodePathList().size());

      // delete the nested inode
      deleteInodeByPath(mTree, NESTED_URI);

      // only /nested left
      assertEquals(1, mTree.getDescendants(inodePath).getInodePathList().size());
    }
  }

  @Test
  public void setPinned() throws Exception {
    createPath(mTree, NESTED_URI, sNestedDirectoryContext);
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    // no inodes pinned
    assertEquals(0, mTree.getPinIdSet().size());

    // pin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(
            NESTED_URI, LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)
    ) {
      mTree.setPinned(RpcContext.NOOP, inodePath, true, Collections.emptyList(), 0);
    }
    // nested file pinned
    assertEquals(1, mTree.getPinIdSet().size());

    // unpin nested folder
    try (
        LockedInodePath inodePath = mTree.lockFullInodePath(
            NESTED_URI, LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)
    ) {
      mTree.setPinned(RpcContext.NOOP, inodePath, false, Collections.emptyList(), 0);
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
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    verifyJournal(mTree, StreamUtils.map(path -> getInodeByPath(path),
        Arrays.asList("/", "/nested", "/nested/test", "/nested/test/file")));

    // add a sibling of test and verify journaling is in correct order (breadth first)
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileContext);
    verifyJournal(mTree, StreamUtils.map(path -> getInodeByPath(path), Arrays.asList("/",
        "/nested", "/nested/test", "/nested/test1", "/nested/test/file", "/nested/test1/file1")));
  }

  @Test
  public void addInodeFromJournal() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);
    createPath(mTree, new AlluxioURI("/nested/test1/file1"), sNestedFileContext);
    MutableInode<?> root = getInodeByPath("/");
    MutableInode<?> nested = getInodeByPath("/nested");
    MutableInode<?> test = getInodeByPath("/nested/test");
    MutableInode<?> file = getInodeByPath("/nested/test/file");
    MutableInode<?> test1 = getInodeByPath("/nested/test1");
    MutableInode<?> file1 = getInodeByPath("/nested/test1/file1");
    // reset the tree
    mTree.processJournalEntry(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(
                 new AlluxioURI("/"), LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)
    ) {
      assertEquals(0, mTree.getDescendants(inodePath).getInodePathList().size());
      mTree.processJournalEntry(nested.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested"));
      mTree.processJournalEntry(test.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test"));
      mTree.processJournalEntry(test1.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1"));
      mTree.processJournalEntry(file.toJournalEntry());
      verifyChildrenNames(mTree, inodePath, Sets.newHashSet("nested", "test", "test1", "file"));
      mTree.processJournalEntry(file1.toJournalEntry());
      verifyChildrenNames(mTree, inodePath,
          Sets.newHashSet("nested", "test", "test1", "file", "file1"));
    }
  }

  @Test
  public void addInodeModeFromJournalWithEmptyOwnership() throws Exception {
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);
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
    mTree.processJournalEntry(root.toJournalEntry());
    // re-init the root since the tree was reset above
    mTree.getRoot();
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(
                 new AlluxioURI("/"), LockPattern.WRITE_INODE, NoopJournalContext.INSTANCE)
    ) {
      assertEquals(0, mTree.getDescendants(inodePath).getInodePathList().size());
      mTree.processJournalEntry(nested.toJournalEntry());
      mTree.processJournalEntry(test.toJournalEntry());
      mTree.processJournalEntry(file.toJournalEntry());
      List<LockedInodePath> descendants = mTree.getDescendants(inodePath).getInodePathList();
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
    try (LockedInodePath rootPath
             = mTree.lockFullInodePath(0, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      assertEquals(0, rootPath.getInode().getId());
    }

    List<Inode> created = createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    for (Inode inode : created) {
      long id = inode.getId();
      try (LockedInodePath inodePath
               = mTree.lockFullInodePath(id, LockPattern.READ, NoopJournalContext.INSTANCE)) {
        assertEquals(id, inodePath.getInode().getId());
      }
    }
  }

  @Test
  public void getInodePathByPath() throws Exception {
    try (LockedInodePath rootPath =
        mTree.lockFullInodePath(
            new AlluxioURI("/"), LockPattern.READ, NoopJournalContext.INSTANCE)
    ) {
      assertTrue(mTree.isRootId(rootPath.getInode().getId()));
    }

    // Create a nested file.
    createPath(mTree, NESTED_FILE_URI, sNestedFileContext);

    AlluxioURI uri = new AlluxioURI("/nested");
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(uri, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_URI;
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(uri, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }

    uri = NESTED_FILE_URI;
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(uri, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      assertEquals(uri.getName(), inodePath.getInode().getName());
    }
  }

  // Helper to create a path.
  private List<Inode> createPath(InodeTree root, AlluxioURI path, CreatePathContext<?, ?> context)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    try (LockedInodePath inodePath =
             root.lockInodePath(path, LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)) {
      return root.createPath(RpcContext.NOOP, inodePath, context);
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
    try (LockedInodePath inodePath =
             mTree.lockFullInodePath(path, LockPattern.READ, NoopJournalContext.INSTANCE)) {
      return mInodeStore.getMutable(inodePath.getInode().getId()).get();
    }
  }

  // Helper to delete an inode by path.
  private static void deleteInodeByPath(InodeTree root, AlluxioURI path) throws Exception {
    try (LockedInodePath inodePath
             = root.lockFullInodePath(path, LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)) {
      root.deleteInode(RpcContext.NOOP, inodePath, System.currentTimeMillis());
    }
  }

  // helper for verifying that correct objects were journaled to the output stream
  private static void verifyJournal(InodeTree root, List<MutableInode<?>> journaled) {
    try (CloseableIterator<Journal.JournalEntry> it = root.getJournalEntryIterator()) {
      // Read entries from InodeTree.
      List<Journal.JournalEntry> treeEntries = new LinkedList<>();
      while (it.get().hasNext()) {
        treeEntries.add(it.get().next());
      }

      // Validate InodeTree entries match given entries.
      for (MutableInode<?> node : journaled) {
        assertTrue(treeEntries.contains(node.toJournalEntry()));
      }
    }
  }

  // verify that the tree has the given children
  private static void verifyChildrenNames(InodeTree tree, LockedInodePath inodePath,
      Set<String> childNames) throws Exception {
    List<LockedInodePath> descendants = tree.getDescendants(inodePath).getInodePathList();
    assertEquals(childNames.size(), descendants.size());
    for (LockedInodePath childPath : descendants) {
      assertTrue(childNames.contains(childPath.getInode().getName()));
    }
  }
}
