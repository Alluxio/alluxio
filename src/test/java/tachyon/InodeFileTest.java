package tachyon;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Unit tests for InodeFile
 */
public class InodeFileTest {
  @Test
  public void inodeLengthTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    long testLength = 100L;
    inodeFile.setLength(testLength);
    Assert.assertEquals(testLength, inodeFile.getLength());
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeInvalidLengthTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    inodeFile.setLength(-100L);
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeRepeatedLengthSetTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    inodeFile.setLength(100L);
    inodeFile.setLength(200L);
  }

  @Test
  public void isReadyTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.isReady());
    inodeFile.setLength(100L);
    Assert.assertTrue(inodeFile.isReady());
  }

  @Test
  public void inMemoryLocationsTest() throws IOException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    List<NetAddress> testAddresses = new ArrayList<NetAddress>(3);
    testAddresses.add(new NetAddress("testhost1", 1000));
    testAddresses.add(new NetAddress("testhost2", 2000));
    testAddresses.add(new NetAddress("testhost3", 3000));
    inodeFile.addLocation(1, testAddresses.get(0));
    Assert.assertEquals(inodeFile.getLocations().size(), 1);
    inodeFile.addLocation(2, testAddresses.get(1));
    Assert.assertEquals(inodeFile.getLocations().size(), 2);
    inodeFile.addLocation(3, testAddresses.get(2));
    Assert.assertEquals(inodeFile.getLocations().size(), 3);
    Assert.assertEquals(inodeFile.getLocations(), testAddresses);
  }

  @Test
  public void inMemoryTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.isInMemory());
    inodeFile.addLocation(1, new NetAddress("testhost1", 1000));
    Assert.assertTrue(inodeFile.isInMemory());
    inodeFile.removeLocation(1);
    Assert.assertFalse(inodeFile.isInMemory());
    inodeFile.addLocation(1, new NetAddress("testhost1", 1000));
    inodeFile.addLocation(1, new NetAddress("testhost1", 1000));
    Assert.assertTrue(inodeFile.isInMemory());
    inodeFile.removeLocation(1);
    Assert.assertFalse(inodeFile.isInMemory());
    inodeFile.addLocation(1, new NetAddress("testhost1", 1000));
    inodeFile.addLocation(2, new NetAddress("testhost1", 1000));
    Assert.assertTrue(inodeFile.isInMemory());
    inodeFile.removeLocation(1);
    Assert.assertTrue(inodeFile.isInMemory());
  }

  @Test
  public void setCheckpointPathTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.hasCheckpointed());
    Assert.assertEquals(inodeFile.getCheckpointPath(), "");
    inodeFile.setCheckpointPath("/testPath");
    Assert.assertEquals(inodeFile.getCheckpointPath(), "/testPath");
  }

  @Test
  public void notInMemoryLocationsTest() throws IOException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    inodeFile.setCheckpointPath("/testPath");
    Assert.assertTrue(inodeFile.getLocations().size() > 0);
  }

  @Test
  public void setPinTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.isPin());
    inodeFile.setPin(true);
    Assert.assertTrue(inodeFile.isPin());
    inodeFile.setPin(false);
    Assert.assertFalse(inodeFile.isPin());
  }

  @Test
  public void setCacheTest() {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.isCache());
    inodeFile.setCache(true);
    Assert.assertTrue(inodeFile.isCache());
    inodeFile.setCache(false);
    Assert.assertFalse(inodeFile.isCache());
  }

  //Tests for Inode methods
  @Test
  public void comparableTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    InodeFile inode2 = new InodeFile("test2", 2, 0);
    Assert.assertEquals(inode1.compareTo(inode2), -1);
    Assert.assertEquals(inode1.compareTo(inode1), 0);
    Assert.assertEquals(inode2.compareTo(inode2), 0);
    Assert.assertEquals(inode2.compareTo(inode1), 1);
  }

  @Test
  public void equalsTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    InodeFile inode2 = new InodeFile("test2", 1, 0);
    Assert.assertTrue(inode1.equals(inode2));
  }

  @Test
  public void isDirectoryTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertFalse(inode1.isDirectory());
  }

  @Test
  public void isFileTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertTrue(inode1.isFile());
  }

  @Test
  public void getInodeTypeTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(inode1.getInodeType(), InodeType.File);
  }

  @Test
  public void getIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(inode1.getId(), 1);
  }

  @Test
  public void reverseIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    inode1.reverseId();
    Assert.assertEquals(inode1.getId(), -1);
  }

  @Test
  public void setNameTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(inode1.getName(), "test1");
    inode1.setName("test2");
    Assert.assertEquals(inode1.getName(), "test2");
  }

  @Test
  public void setParentIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(inode1.getParentId(), 0);
    inode1.setParentId(2);
    Assert.assertEquals(inode1.getParentId(), 2);
  } 
}