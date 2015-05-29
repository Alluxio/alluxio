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
package tachyon.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.master.permission.Acl;
import tachyon.master.permission.AclUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

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
  public void setLastModificationTimeTest() {
    long createTimeMs = System.currentTimeMillis();
    long modificationTimeMs = createTimeMs + 1000;
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, createTimeMs);
    Assert.assertEquals(createTimeMs, inodeFolder.getLastModificationTimeMs());
    inodeFolder.setLastModificationTimeMs(modificationTimeMs);
    Assert.assertEquals(modificationTimeMs, inodeFolder.getLastModificationTimeMs());
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

  @Test
  public void writeImageTest() throws IOException {
    // create the InodeFolder and the output streams
    long creationTime = System.currentTimeMillis();
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, creationTime);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    // write the image
    inode1.writeImage(writer, dos);

    // decode the written bytes
    ImageElement decoded = mapper.readValue(os.toByteArray(), ImageElement.class);

    // test the decoded ImageElement
    Assert.assertEquals(creationTime, decoded.getLong("creationTimeMs").longValue());
    Assert.assertEquals(1, decoded.getInt("id").intValue());
    Assert.assertEquals("test1", decoded.getString("name"));
    Assert.assertEquals(0, decoded.getInt("parentId").intValue());
    Assert.assertEquals(new ArrayList<Integer>(),
        decoded.get("childrenIds", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(creationTime, decoded.getLong("lastModificationTimeMs").longValue());
  }

  @Test
  public void getChildTest() {
    // large number of small files
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0, System.currentTimeMillis());
    int nFiles = (int) 1E5;
    Inode[] inodes = new Inode[nFiles];
    for (int i = 0; i < nFiles; i ++) {
      inodes[i] =
          new InodeFile(String.format("testFile%d", i + 1), i + 2, 1, 1, System.currentTimeMillis());
      inodeFolder.addChild(inodes[i]);
    }

    Runtime runtime = Runtime.getRuntime();
    System.out.println(String.format("Used Memory = %dB when number of files = %d",
        runtime.totalMemory() - runtime.freeMemory(), nFiles));

    long start = System.currentTimeMillis();
    for (int i = 0; i < nFiles; i ++) {
      Assert.assertEquals(inodes[i], inodeFolder.getChild(i + 2));
    }
    System.out.println(String.format("getChild(int fid) called sequentially %d times, cost %d ms",
        nFiles, System.currentTimeMillis() - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < nFiles; i ++) {
      Assert.assertEquals(inodes[i], inodeFolder.getChild(String.format("testFile%d", i + 1)));
    }
    System.out.println(String.format(
        "getChild(String name) called sequentially %d times, cost %d ms", nFiles,
        System.currentTimeMillis() - start));
  }

  @Test
  public void setAclTest() {
    Acl acl = AclUtil.getAcl("test1", "test1", (short)0755);
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis(), acl);
    Assert.assertEquals(inodeFile.getAcl().getUserName(), "test1");
    Assert.assertEquals(inodeFile.getAcl().getGroupName(), "test1");
    Assert.assertEquals(inodeFile.getAcl().toShort(), 0755);
    inodeFile.getAcl().setGroupOwner("test3");
    inodeFile.getAcl().setUserOwner("test2");
    inodeFile.getAcl().setPermission((short)0777);
    Assert.assertEquals(inodeFile.getAcl().getUserName(), "test2");
    Assert.assertEquals(inodeFile.getAcl().getGroupName(), "test3");
    Assert.assertEquals(inodeFile.getAcl().toShort(), 0777);
  }
}
