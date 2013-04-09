package tachyon;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for InodeRawTable
 */
public class InodeRawTableTest {
  @Test
  public void getColumnsTest() {
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inodeRawTable.getColumns(), 10);
  }

  @Test
  public void getNullMetadataTest() {
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inodeRawTable.getMetadata().equals(ByteBuffer.allocate(0)));
  }

  @Test
  public void getMetadataTest() {
    ByteBuffer metadata = ByteBuffer.allocate(8);
    metadata.putInt(1);
    metadata.putInt(2);
    metadata.flip();
    InodeRawTable inodeRawTable = new InodeRawTable("testTable1", 1, 0, 10, metadata);
    metadata.flip();
    Assert.assertTrue(inodeRawTable.getMetadata().equals(metadata));
  }

  //Tests for Inode methods
  @Test
  public void comparableTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode2 = new InodeRawTable("test2", 2, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.compareTo(inode2), -1);
    Assert.assertEquals(inode1.compareTo(inode1), 0);
    Assert.assertEquals(inode2.compareTo(inode2), 0);
    Assert.assertEquals(inode2.compareTo(inode1), 1);
  }

  @Test
  public void equalsTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode2 = new InodeRawTable("test2", 1, 0, 10, (ByteBuffer) null);
    InodeRawTable inode3 = new InodeRawTable("test3", 2, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inode1.equals(inode2));
    Assert.assertFalse(inode1.equals(inode3));
  }

  @Test
  public void isDirectoryTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertTrue(inode1.isDirectory());
  }

  @Test
  public void isFileTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertFalse(inode1.isFile());
  }

  @Test
  public void getInodeTypeTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.getInodeType(), InodeType.RawTable);
  }

  @Test
  public void getIdTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.getId(), 1);
  }

  @Test
  public void reverseIdTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    inode1.reverseId();
    Assert.assertEquals(inode1.getId(), -1);
  }

  @Test
  public void setNameTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.getName(), "test1");
    inode1.setName("test2");
    Assert.assertEquals(inode1.getName(), "test2");
  }

  @Test
  public void setParentIdTest() {
    InodeRawTable inode1 = new InodeRawTable("test1", 1, 0, 10, (ByteBuffer) null);
    Assert.assertEquals(inode1.getParentId(), 0);
    inode1.setParentId(2);
    Assert.assertEquals(inode1.getParentId(), 2);
  } 
}