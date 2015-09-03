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

package tachyon.master.filesystem.meta;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.block.BlockMaster;
import tachyon.master.journal.Journal;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for InodeTree.
 */
public final class InodeTreeTest {
  private static final String TEST_PATH = "test";
  private static final TachyonURI TEST_URI = new TachyonURI("/test");
  private static final TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");
  private InodeTree mTree;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws IOException {
    TachyonConf conf = new TachyonConf();
    Journal blockJournal = new Journal(mTestFolder.newFolder().getAbsolutePath(), conf);

    BlockMaster blockMaster = new BlockMaster(blockJournal, conf);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    mTree = new InodeTree(blockMaster, directoryIdGenerator);

    blockMaster.start(true);
    mTree.initializeRoot();
  }

  @Test
  public void createDirectoryTest() throws Exception {
    // create directory
    mTree.createPath(TEST_URI, Constants.KB, false, true);
    Inode test = mTree.getInodeByPath(TEST_URI);
    Assert.assertEquals(TEST_PATH, test.getName());
    Assert.assertTrue(test.isDirectory());

    // create nested directory
    mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Inode nested = mTree.getInodeByPath(NESTED_URI);
    Assert.assertEquals(TEST_PATH, nested.getName());
    Assert.assertEquals(2, nested.getParentId());
    Assert.assertTrue(test.isDirectory());
  }

  @Test
  public void createFileTest() throws Exception {
    // created nested file
    mTree.createPath(NESTED_FILE_URI, Constants.KB, true, false);
    Inode nestedFile = mTree.getInodeByPath(NESTED_FILE_URI);
    Assert.assertEquals("file", nestedFile.getName());
    Assert.assertEquals(2, nestedFile.getParentId());
    Assert.assertTrue(nestedFile.isFile());
  }

  @Test
  public void createPathTest() throws Exception {
    // create nested directory
    List<Inode> inodes = mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Assert.assertEquals(2, inodes.size());
    Assert.assertEquals("nested", inodes.get(0).getName());
    Assert.assertEquals("test", inodes.get(1).getName());

    // creating the directory path again results in no new inodes.
    inodes = mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Assert.assertEquals(0, inodes.size());

    // create a file
    inodes = mTree.createPath(NESTED_FILE_URI, Constants.KB, true, false);
    Assert.assertEquals(1, inodes.size());
    Assert.assertEquals("file", inodes.get(0).getName());
  }

  @Test
  public void createRootPathTest() throws Exception {
    mThrown.expect(FileAlreadyExistException.class);
    mThrown.expectMessage("/");

    mTree.createPath(new TachyonURI("/"), Constants.KB, false, true);
  }

  @Test
  public void createFileWithInvalidBlockSizeTest() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size 0");

    mTree.createPath(TEST_URI, 0, false, false);
  }

  @Test
  public void createFileWithNegativeBlockSizeTest() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size -1");

    mTree.createPath(TEST_URI, -1, false, false);
  }

  @Test
  public void createFileUnderNonexistingDirTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("File /nested/test creation failed. Component 1(nested) does not exist");

    mTree.createPath(NESTED_URI, Constants.KB, false, false);
  }

  @Test
  public void createFileTwiceTest() throws Exception {
    mThrown.expect(FileAlreadyExistException.class);
    mThrown.expectMessage("/nested/test");

    mTree.createPath(NESTED_URI, Constants.KB, true, false);
    mTree.createPath(NESTED_URI, Constants.KB, true, false);
  }

  @Test
  public void createFileUnderFileTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not traverse to parent directory of path /nested/test/test."
        + " Component test is not a directory.");

    mTree.createPath(NESTED_URI, Constants.KB, true, false);
    mTree.createPath(new TachyonURI("/nested/test/test"), Constants.KB, true, false);
  }

  @Test
  public void getInodeByNonexistingPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /test");

    mTree.getInodeByPath(TEST_URI);
  }

  @Test
  public void getInodeByNonexistingNestedPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /nested/test/file");

    mTree.createPath(NESTED_URI, Constants.KB, true, true);
    mTree.getInodeByPath(NESTED_FILE_URI);
  }

  @Test
  public void getInodeByInvalidIdTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Inode id 1 does not exist");

    mTree.getInodeById(1);
  }

  @Test
  public void isRootIdTest() throws Exception {
    Assert.assertTrue(mTree.isRootId(0));
    Assert.assertFalse(mTree.isRootId(1));
  }

  @Test
  public void getPathTest() throws Exception {
    Inode root = mTree.getInodeById(0);
    // test root path
    Assert.assertEquals(new TachyonURI("/"), mTree.getPath(root));

    // test one level
    List<Inode> created = mTree.createPath(TEST_URI, Constants.KB, false, true);
    Assert.assertEquals(new TachyonURI("/test"), mTree.getPath(created.get(created.size() - 1)));

    // test nesting
    created = mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Assert.assertEquals(new TachyonURI("/nested/test"), mTree.getPath(created.get(
        created.size() - 1)));
  }

  @Test
  public void getInodeChildrenRecursiveTest() throws Exception {
    mTree.createPath(TEST_URI, Constants.KB, false, true);
    mTree.createPath(NESTED_URI, Constants.KB, true, true);
    // add nested file
    mTree.createPath(NESTED_FILE_URI, Constants.KB, true, false);

    // all inodes under root
    List<Inode> inodes = mTree.getInodeChildrenRecursive((InodeDirectory) mTree.getInodeById(0));
    // /test, /nested, /nested/test, /nested/test/file
    Assert.assertEquals(4, inodes.size());
  }

  @Test
  public void deleteInodeTest() throws Exception {
    List<Inode> created = mTree.createPath(NESTED_URI, Constants.KB, true, true);

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

  @Test
  public void deleteNonexistingInodeTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage("Inode id 1 does not exist");

    Inode testFile = new InodeFile("testFile1", 1, 1, Constants.KB, System.currentTimeMillis());
    mTree.deleteInode(testFile);
  }

  @Test
  public void setPinnedTest() throws Exception {
    List<Inode> created = mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Inode nested = created.get(created.size() - 1);
    mTree.createPath(NESTED_FILE_URI, Constants.KB, true, false);

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
}
