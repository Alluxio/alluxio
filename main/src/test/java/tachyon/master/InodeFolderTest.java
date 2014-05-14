/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import org.junit.Assert;
import org.junit.Test;

import tachyon.master.InodeFile;
import tachyon.master.InodeFolder;

/**
 * Unit tests for tachyon.InodeFolder
 */
public class InodeFolderTest {
  @Test
  public void addChildrenTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1, 1000, System.currentTimeMillis());
    InodeFile inodeFile2 = new InodeFile("testFile2", 3, 1, 1000, System.currentTimeMillis());
    inodeFolder.addChild(inodeFile1);
    inodeFolder.addChild(inodeFile2);
    Assert.assertEquals(2, (int) inodeFolder.getChildrenIds().get(0));
    Assert.assertEquals(3, (int) inodeFolder.getChildrenIds().get(1));
  }

  @Test
  public void batchRemoveChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1, 1000, System.currentTimeMillis());
    InodeFile inodeFile2 = new InodeFile("testFile2", 3, 1, 1000, System.currentTimeMillis());
    InodeFile inodeFile3 = new InodeFile("testFile3", 4, 1, 1000, System.currentTimeMillis());
    inodeFolder.addChild(inodeFile1);
    inodeFolder.addChild(inodeFile2);
    inodeFolder.addChild(inodeFile3);
    Assert.assertEquals(3, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild("testFile1");
    Assert.assertEquals(2, inodeFolder.getNumberOfChildren());
    Assert.assertFalse(inodeFolder.getChildrenIds().contains(2));
  }

  // Tests for Inode methods
  @Test
  public void comparableTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    InodeFolder inode2 = new InodeFolder("test2", 2, 0, System.currentTimeMillis());
    Assert.assertEquals(-1, inode1.compareTo(inode2));
  }

  @Test
  public void equalsTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    InodeFolder inode2 = new InodeFolder("test2", 1, 0, System.currentTimeMillis());
    Assert.assertTrue(inode1.equals(inode2));
  }

  @Test
  public void getIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void isDirectoryTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    Assert.assertTrue(inode1.isDirectory());
  }

  @Test
  public void isFileTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    Assert.assertFalse(inode1.isFile());
  }

  @Test
  public void removeChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1, 1000, System.currentTimeMillis());
    inodeFolder.addChild(inodeFile1);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild(inodeFile1);
    Assert.assertEquals(0, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void removeNonExistentChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1, 1000, System.currentTimeMillis());
    InodeFile inodeFile2 = new InodeFile("testFile2", 3, 1, 1000, System.currentTimeMillis());
    inodeFolder.addChild(inodeFile1);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild(inodeFile2);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void reverseIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void sameIdChildrenTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1, 1000, System.currentTimeMillis());
    inodeFolder.addChild(inodeFile1);
    inodeFolder.addChild(inodeFile1);
    Assert.assertTrue(inodeFolder.getChildrenIds().get(0) == 2);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void setNameTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, System.currentTimeMillis());
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }
}
