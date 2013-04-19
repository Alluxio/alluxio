package tachyon;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Unit tests for tachyon.InodeFile
 */
public class InodeFileTest {
  @Test
  public void inodeLengthTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    long testLength = 100;
    inodeFile.setLength(testLength);
    Assert.assertEquals(testLength, inodeFile.getLength());
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeInvalidLengthTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    inodeFile.setLength(-100);
  }

  @Test(expected = SuspectedFileSizeException.class)
  public void inodeRepeatedLengthSetTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    inodeFile.setLength(100);
    inodeFile.setLength(200);
  }

  @Test
  public void isReadyTest() throws SuspectedFileSizeException {
    InodeFile inodeFile = new InodeFile("testFile1", 1, 0);
    Assert.assertFalse(inodeFile.isReady());
    inodeFile.setLength(100);
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
    Assert.assertEquals(1, inodeFile.getLocations().size());
    inodeFile.addLocation(2, testAddresses.get(1));
    Assert.assertEquals(2, inodeFile.getLocations().size());
    inodeFile.addLocation(3, testAddresses.get(2));
    Assert.assertEquals(3, inodeFile.getLocations().size());
    Assert.assertEquals(testAddresses, inodeFile.getLocations());
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
    Assert.assertEquals("", inodeFile.getCheckpointPath());
    inodeFile.setCheckpointPath("/testPath");
    Assert.assertEquals("/testPath", inodeFile.getCheckpointPath());
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
    Assert.assertEquals(-1, inode1.compareTo(inode2));
    Assert.assertEquals(0, inode1.compareTo(inode1));
    Assert.assertEquals(0, inode2.compareTo(inode2));
    Assert.assertEquals(1, inode2.compareTo(inode1));
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
    Assert.assertEquals(InodeType.File, inode1.getInodeType());
  }

  @Test
  public void getIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void reverseIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void setNameTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() {
    InodeFile inode1 = new InodeFile("test1", 1, 0);
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  } 
}