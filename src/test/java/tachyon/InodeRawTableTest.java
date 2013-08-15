package tachyon;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.TachyonException;

/**
 * Unit tests for tachyon.InodeRawTable
 */
public class InodeRawTableTest {
  @Test
  public void getColumnsTest() throws TachyonException {
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(10, inodeRawTable.getColumns());
  }

  @Test
  public void getNullMetadataTest() throws TachyonException {
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inodeRawTable.getMetadata().equals(ByteBuffer.allocate(0)));
  }

  @Test
  public void getMetadataTest() throws TachyonException {
    ByteBuffer metadata = TestUtils.getIncreasingIntBuffer(3);
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, metadata);
    Assert.assertEquals(metadata, inodeRawTable.getMetadata());
  }

  @Test
  public void updateMetadataTest() throws TachyonException {
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, null);
    Assert.assertEquals(ByteBuffer.allocate(0), inodeRawTable.getMetadata());
    ByteBuffer metadata = TestUtils.getIncreasingIntBuffer(7);
    inodeRawTable.updateMetadata(metadata);
    Assert.assertEquals(metadata, inodeRawTable.getMetadata());
  }

  //Tests for Inode methods
  @Test
  public void comparableTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode2 = new InodeRawTable("test2", 2, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(-1, inode1.compareTo(inode2));
    Assert.assertEquals(0, inode1.compareTo(inode1));
    Assert.assertEquals(0, inode2.compareTo(inode2));
    Assert.assertEquals(1, inode2.compareTo(inode1));
  }

  @Test
  public void equalsTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode2 = new InodeRawTable("test2", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode3 = new InodeRawTable("test3", 2, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inode1.equals(inode2));
    Assert.assertFalse(inode1.equals(inode3));
  }

  @Test
  public void isDirectoryTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inode1.isDirectory());
  }

  @Test
  public void isFileTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertFalse(inode1.isFile());
  }

  @Test
  public void getInodeTypeTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.getInodeType(), InodeType.RawTable);
  }

  @Test
  public void getIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void reverseIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void setNameTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() throws TachyonException {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  } 
}