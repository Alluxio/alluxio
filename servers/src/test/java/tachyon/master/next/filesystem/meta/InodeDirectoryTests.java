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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.block.BlockId;

/**
 * Unit tests for tachyon.InodeDirectory
 */
public final class InodeDirectoryTests {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  
  @Test
  public void addChildrenTest() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(2);
    InodeFile inodeFile2 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile2);
    Assert.assertEquals(createBlockId(2), (long) inodeDirectory.getChildrenIds().get(0));
    Assert.assertEquals(createBlockId(3), (long) inodeDirectory.getChildrenIds().get(1));
  }

  @Test
  public void batchRemoveChildTest() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    InodeFile inodeFile2 = createInodeFile(2);
    InodeFile inodeFile3 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile2);
    inodeDirectory.addChild(inodeFile3);
    Assert.assertEquals(3, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild("testFile1");
    Assert.assertEquals(2, inodeDirectory.getNumberOfChildren());
    Assert.assertFalse(inodeDirectory.getChildrenIds().contains(createBlockId(1)));
  }

  @Test
  public void equalsTest() {
    InodeDirectory inode1 = new InodeDirectory("test1", 1, 0, System.currentTimeMillis());
    InodeDirectory inode2 = new InodeDirectory("test2", 1, 0, System.currentTimeMillis());
    Assert.assertTrue(inode1.equals(inode2));
  }

  @Test
  public void getIdTest() {
    Assert.assertEquals(1, createInodeDirectory().getId());
  }

  @Test
  public void isDirectoryTest() {
    Assert.assertTrue(createInodeDirectory().isDirectory());
  }

  @Test
  public void isFileTest() {
    Assert.assertFalse(createInodeDirectory().isFile());
  }

  @Test
  public void removeChildTest() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild(inodeFile1);
    Assert.assertEquals(0, inodeDirectory.getNumberOfChildren());
  }

  @Test
  public void removeNonExistentChildTest() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(2);
    InodeFile inodeFile2 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild(inodeFile2);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
  }

  @Test
  public void reverseIdTest() {
    InodeDirectory inode1 = createInodeDirectory();
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void sameIdChildrenTest() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertTrue(inodeDirectory.getChildrenIds().get(0) == createBlockId(1));
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
  }

  @Test
  public void setLastModificationTimeTest() {
    long createTimeMs = System.currentTimeMillis();
    long modificationTimeMs = createTimeMs + 1000;
    InodeDirectory inodeDirectory = createInodeDirectory();
    Assert.assertEquals(createTimeMs, inodeDirectory.getLastModificationTimeMs());
    inodeDirectory.setLastModificationTimeMs(modificationTimeMs);
    Assert.assertEquals(modificationTimeMs, inodeDirectory.getLastModificationTimeMs());
  }

  @Test
  public void setNameTest() {
    InodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() {
    InodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }

  @Test
  public void getChildTest() {
    // large number of small files
    InodeDirectory inodeDirectory = createInodeDirectory();
    int nFiles = (int) 1E5;
    Inode[] inodes = new Inode[nFiles];
    for (int i = 0; i < nFiles; i ++) {
      inodes[i] = createInodeFile(i+1);
      inodeDirectory.addChild(inodes[i]);
    }

    Runtime runtime = Runtime.getRuntime();
    LOG.info(String.format("Used Memory = %dB when number of files = %d",
        runtime.totalMemory() - runtime.freeMemory(), nFiles));

    long start = System.currentTimeMillis();
    for (int i = 0; i < nFiles; i ++) {
      Assert.assertEquals(inodes[i], inodeDirectory.getChild(createBlockId(i + 1)));
    }
    LOG.info(String.format("getChild(int fid) called sequentially %d times, cost %d ms", nFiles,
        System.currentTimeMillis() - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < nFiles; i ++) {
      Assert.assertEquals(inodes[i], inodeDirectory.getChild(String.format("testFile%d", i + 1)));
    }
    LOG.info(String.format("getChild(String name) called sequentially %d times, cost %d ms", nFiles,
        System.currentTimeMillis() - start));
  }

  private long createBlockId(long containerId) {
    return BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
  }
  
  private static InodeDirectory createInodeDirectory() {
    return new InodeDirectory("test1", 1, 0, System.currentTimeMillis());
  }
  
  private InodeFile createInodeFile(long id) {
    return new InodeFile("testFile"+id, id, 1, 1000, System.currentTimeMillis()); 
  }
}
