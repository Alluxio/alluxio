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

package tachyon.master.next.filesystem.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.master.next.block.BlockMaster;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for InodeTree.
 */
public final class InodeTreeTests {
  private static String TEST_PATH = "test";
  private static TachyonURI TEST_URI = new TachyonURI("/test");
  private static TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private InodeTree mTree;
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mTree = new InodeTree(new BlockMaster());
  }

  @Test
  public void createPathTest() throws Exception {
    // create directory
    mTree.createPath(TEST_URI, Constants.KB, false, true);
    Inode test = mTree.getInodeByPath(TEST_URI);
    Assert.assertEquals(TEST_PATH, test.getName());

    // create nested directory
    mTree.createPath(NESTED_URI, Constants.KB, true, true);
    Inode nested = mTree.getInodeByPath(NESTED_URI);
    Assert.assertEquals(TEST_PATH, nested.getName());
    Assert.assertEquals(2, nested.getParentId());
  }

  @Test
  public void createRootPathTest() throws Exception {
    mThrown.expect(FileAlreadyExistException.class);
    mThrown.expectMessage("/");

    mTree.createPath(new TachyonURI("/"), Constants.KB, false, true);
  }

  @Test
  public void createEmptyFileTest() throws Exception {
    mThrown.expect(BlockInfoException.class);
    mThrown.expectMessage("Invalid block size 0");

    mTree.createPath(TEST_URI, 0, false, false);
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
    mThrown.expectMessage(
        "Could not traverse to parent directory of path /nested/test/test. Component test is not a directory.");

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
}
