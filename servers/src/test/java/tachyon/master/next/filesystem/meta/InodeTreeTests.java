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

/**
 * Unit tests for InodeTree.
 */
public final class InodeTreeTests {
  private static TachyonURI TEST_URI = new TachyonURI("/test");
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
    Inode test = mTree.getInodeByPath(new TachyonURI("/test"));
    Assert.assertEquals(1, test.getId());
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
}
