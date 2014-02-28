/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.master.InodeRawTable;
import tachyon.master.InodeType;
import tachyon.thrift.TachyonException;

/**
 * Unit tests for tachyon.InodeRawTable
 */
public class InodeRawTableTest {
  // Tests for Inode methods
  @Test
  public void comparableTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    InodeRawTable inode2 = new InodeRawTable("test2", 2, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(-1, inode1.compareTo(inode2));
    Assert.assertEquals(0, inode1.compareTo(inode1));
    Assert.assertEquals(0, inode2.compareTo(inode2));
    Assert.assertEquals(1, inode2.compareTo(inode1));
  }

  @Test
  public void equalsTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    InodeRawTable inode2 = new InodeRawTable("test2", 1, 0, 10, null, System.currentTimeMillis());
    InodeRawTable inode3 = new InodeRawTable("test3", 2, 0, 10, null, System.currentTimeMillis());
    Assert.assertTrue(inode1.equals(inode2));
    Assert.assertFalse(inode1.equals(inode3));
  }

  @Test
  public void getColumnsTest() throws TachyonException {
    InodeRawTable inodeRawTable =
        new InodeRawTable("testTable1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(10, inodeRawTable.getColumns());
  }

  @Test
  public void getIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void getInodeTypeTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(inode1.getInodeType(), InodeType.RawTable);
  }

  @Test
  public void getMetadataTest() throws TachyonException {
    ByteBuffer metadata = TestUtils.getIncreasingIntBuffer(3);
    InodeRawTable inodeRawTable =
        new InodeRawTable("testTable1", 1, 0, 10, metadata, System.currentTimeMillis());
    Assert.assertEquals(metadata, inodeRawTable.getMetadata());
  }

  @Test
  public void getNullMetadataTest() throws TachyonException {
    InodeRawTable inodeRawTable =
        new InodeRawTable("testTable1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertTrue(inodeRawTable.getMetadata().equals(ByteBuffer.allocate(0)));
  }

  @Test
  public void isDirectoryTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertTrue(inode1.isDirectory());
  }

  @Test
  public void isFileTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertFalse(inode1.isFile());
  }

  @Test
  public void reverseIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void setNameTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }

  @Test
  public void updateMetadataTest() throws TachyonException {
    InodeRawTable inodeRawTable =
        new InodeRawTable("testTable1", 1, 0, 10, null, System.currentTimeMillis());
    Assert.assertEquals(ByteBuffer.allocate(0), inodeRawTable.getMetadata());
    ByteBuffer metadata = TestUtils.getIncreasingIntBuffer(7);
    inodeRawTable.updateMetadata(metadata);
    Assert.assertEquals(metadata, inodeRawTable.getMetadata());
  }
}