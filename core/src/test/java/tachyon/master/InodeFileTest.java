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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.master.permission.Acl;
import tachyon.master.permission.AclUtil;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Unit tests for tachyon.InodeFile
 */
public class InodeFileTest {
  private TachyonConf mTachyonConf;

  @Before
  public void before() {
    mTachyonConf = new TachyonConf();
  }

  // Tests for Inode methods
  @Test
  public void comparableTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    InodeFile inode2 = new InodeFile("test2", 2, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals(-1, inode1.compareTo(inode2));
    Assert.assertEquals(0, inode1.compareTo(inode1));
    Assert.assertEquals(0, inode2.compareTo(inode2));
    Assert.assertEquals(1, inode2.compareTo(inode1));
  }

  @Test
  public void equalsTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    InodeFile inode2 = new InodeFile("test2", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertTrue(inode1.equals(inode2));
  }

  @Test
  public void getIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void inMemoryLocationsTest() throws IOException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    List<NetAddress> testAddresses = new ArrayList<NetAddress>(3);
    long storageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    testAddresses.add(new NetAddress("testhost1", 1000, 1001));
    testAddresses.add(new NetAddress("testhost2", 2000, 2001));
    testAddresses.add(new NetAddress("testhost3", 3000, 3001));
    inodeFile.addBlock(new BlockInfo(inodeFile, 0, 5));
    inodeFile.addLocation(0, 1, testAddresses.get(0), storageDirId);
    Assert.assertEquals(1, inodeFile.getBlockLocations(0, mTachyonConf).size());
    inodeFile.addLocation(0, 2, testAddresses.get(1), storageDirId);
    Assert.assertEquals(2, inodeFile.getBlockLocations(0, mTachyonConf).size());
    inodeFile.addLocation(0, 3, testAddresses.get(2), storageDirId);
    Assert.assertEquals(3, inodeFile.getBlockLocations(0, mTachyonConf).size());
    Assert.assertEquals(testAddresses, inodeFile.getBlockLocations(0, mTachyonConf));
  }

  @Test(expected = BlockInfoException.class)
  public void inMemoryLocationsTestWithBlockInfoException() throws IOException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    long storageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    inodeFile.addLocation(0, 1, new NetAddress("testhost1", 1000, 1001), storageDirId);
  }

  @Test
  public void inMemoryTest() throws BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    inodeFile.addBlock(new BlockInfo(inodeFile, 0, 5));
    long storageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    Assert.assertFalse(inodeFile.isFullyInMemory());
    inodeFile.addLocation(0, 1, new NetAddress("testhost1", 1000, 1001), storageDirId);
    Assert.assertTrue(inodeFile.isFullyInMemory());
    inodeFile.removeLocation(0, 1);
    Assert.assertFalse(inodeFile.isFullyInMemory());
    inodeFile.addLocation(0, 1, new NetAddress("testhost1", 1000, 1001), storageDirId);
    inodeFile.addLocation(0, 1, new NetAddress("testhost1", 1000, 1001), storageDirId);
    Assert.assertTrue(inodeFile.isFullyInMemory());
    inodeFile.removeLocation(0, 1);
    Assert.assertFalse(inodeFile.isFullyInMemory());
    inodeFile.addLocation(0, 1, new NetAddress("testhost1", 1000, 1001), storageDirId);
    inodeFile.addLocation(0, 2, new NetAddress("testhost1", 1001, 1002), storageDirId);
    Assert.assertTrue(inodeFile.isFullyInMemory());
    inodeFile.removeLocation(0, 1);
    Assert.assertTrue(inodeFile.isFullyInMemory());
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeInvalidLengthTest() throws SuspectedFileSizeException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    inodeFile.setLength(-100);
  }

  @Test
  public void inodeLengthTest() throws SuspectedFileSizeException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    long testLength = 100;
    inodeFile.setLength(testLength);
    Assert.assertEquals(testLength, inodeFile.getLength());
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeRepeatedLengthSetTest() throws SuspectedFileSizeException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    inodeFile.setLength(100);
    inodeFile.setLength(200);
  }

  @Test
  public void isCompleteTest() throws SuspectedFileSizeException, BlockInfoException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertFalse(inodeFile.isComplete());
    inodeFile.setComplete();
    Assert.assertTrue(inodeFile.isComplete());
  }

  @Test
  public void isDirectoryTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertFalse(inode1.isDirectory());
  }

  @Test
  public void isFileTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertTrue(inode1.isFile());
  }

  @Test
  public void reverseIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void setCacheTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertFalse(inodeFile.isCache());
    inodeFile.setCache(true);
    Assert.assertTrue(inodeFile.isCache());
    inodeFile.setCache(false);
    Assert.assertFalse(inodeFile.isCache());
  }

  @Test
  public void setUfsTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertFalse(inodeFile.hasCheckpointed());
    Assert.assertEquals("", inodeFile.getUfsPath());
    inodeFile.setUfsPath("/testPath");
    Assert.assertEquals("/testPath", inodeFile.getUfsPath());
  }

  @Test
  public void setLastModificationTimeTest() {
    long createTimeMs = System.currentTimeMillis();
    long modificationTimeMs = createTimeMs + 1000;
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, createTimeMs);
    Assert.assertEquals(createTimeMs, inodeFile.getLastModificationTimeMs());
    inodeFile.setLastModificationTimeMs(modificationTimeMs);
    Assert.assertEquals(modificationTimeMs, inodeFile.getLastModificationTimeMs());
  }

  @Test
  public void setNameTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }

  @Test
  public void setPinTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertFalse(inodeFile.isPinned());
    inodeFile.setPinned(true);
    Assert.assertTrue(inodeFile.isPinned());
    inodeFile.setPinned(false);
    Assert.assertFalse(inodeFile.isPinned());
  }

  @Test
  public void setAclTest() {
    Acl acl = AclUtil.getAcl("test1", "test1", (short)0644);
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0, 1000, System.currentTimeMillis(), acl);
    Assert.assertEquals(inodeFile.getAcl().getUserName(), "test1");
    Assert.assertEquals(inodeFile.getAcl().getGroupName(), "test1");
    Assert.assertEquals(inodeFile.getAcl().toShort(), 0644);
    inodeFile.getAcl().setGroupOwner("test3");
    inodeFile.getAcl().setUserOwner("test2");
    inodeFile.getAcl().setPermission((short)0600);
    Assert.assertEquals(inodeFile.getAcl().getUserName(), "test2");
    Assert.assertEquals(inodeFile.getAcl().getGroupName(), "test3");
    Assert.assertEquals(inodeFile.getAcl().toShort(), 0600);
  }

  @Test(expected = BlockInfoException.class)
  public void emeptyInodeGetBlock() throws BlockInfoException {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals(0, inode1.getBlockIds().size());
    // cant get a block that is missing
    inode1.getClientBlockInfo(0, mTachyonConf);
  }

  @Test(expected = BlockInfoException.class)
  public void emeptyInodeGetBlockLarger() throws BlockInfoException {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    Assert.assertEquals(0, inode1.getBlockIds().size());
    // cant get a block that is missing
    inode1.getClientBlockInfo(1, mTachyonConf);
  }

  @Test(expected = BlockInfoException.class)
  public void negativeBlockGetBlock() throws BlockInfoException {
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, System.currentTimeMillis());
    // cant get a block that is missing
    inode1.getClientBlockInfo(-1, mTachyonConf);
  }

  @Test
  public void writeImageTest() throws IOException {
    // create the InodeFile and the output streams
    long creationTime = System.currentTimeMillis();
    InodeFile inode1 = new InodeFile("test1", 1, 0, 1000, creationTime);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    // write the image
    inode1.writeImage(writer, dos);

    // decode the element
    ImageElement decoded = null;
    decoded = mapper.readValue(os.toByteArray(), ImageElement.class);

    // test the decoded image element
    Assert.assertEquals(creationTime, decoded.getLong("creationTimeMs").longValue());
    Assert.assertEquals(1, decoded.getInt("id").intValue());
    Assert.assertEquals(0, decoded.getInt("parentId").intValue());
    Assert.assertEquals(1000, decoded.getInt("blockSizeByte").intValue());
    Assert.assertEquals(0,  decoded.getLong("length").longValue());
    Assert.assertEquals(false, decoded.getBoolean("complete"));
    Assert.assertEquals(false, decoded.getBoolean("pin"));
    Assert.assertEquals(false, decoded.getBoolean("cache"));
    Assert.assertEquals("", decoded.getString("ufsPath"));
    Assert.assertEquals(-1, decoded.getInt("depId").intValue());
    Assert.assertEquals(creationTime, decoded.getLong("lastModificationTimeMs").longValue());
  }
}
