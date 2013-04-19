package tachyon;

import java.util.Map;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for InodeFolder
 */
public class InodeFolderTest {
  @Test
  public void addChildrenTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    inodeFolder.addChild(2);
    inodeFolder.addChild(3);
    Assert.assertEquals(2, (int) inodeFolder.getChildrenIds().get(0));
    Assert.assertEquals(3, (int) inodeFolder.getChildrenIds().get(1));
  }

  @Test
  public void sameIdChildrenTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    inodeFolder.addChild(2);
    inodeFolder.addChild(2);
    Assert.assertTrue(inodeFolder.getChildrenIds().get(0) == 2);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void removeChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    inodeFolder.addChild(2);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild(2);
    Assert.assertEquals(0, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void removeNonExistentChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    inodeFolder.addChild(2);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild(3);
    Assert.assertEquals(1, inodeFolder.getNumberOfChildren());
  }

  @Test
  public void batchRemoveChildTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    InodeFile inodeFile1 = new InodeFile("testFile1", 2, 1);
    InodeFile inodeFile2 = new InodeFile("testFile2", 3, 1);
    inodeFolder.addChild(2);
    inodeFolder.addChild(3);
    inodeFolder.addChild(4);
    Map<Integer, Inode> testMap = new HashMap<Integer, Inode>(2);
    testMap.put(2, inodeFile1);
    testMap.put(3, inodeFile2);
    Assert.assertEquals(3, inodeFolder.getNumberOfChildren());
    inodeFolder.removeChild("testFile1", testMap);
    Assert.assertEquals(2, inodeFolder.getNumberOfChildren());
    Assert.assertFalse(inodeFolder.getChildrenIds().contains(2));
  }

  @Test
  public void isRawTableTest() {
    InodeFolder inodeFolder = new InodeFolder("testFolder1", 1, 0);
    InodeFolder inodeRawTable = new InodeFolder("testRawTable1", 2, 0, InodeType.RawTable);
    Assert.assertFalse(inodeFolder.isRawTable());
    Assert.assertTrue(inodeRawTable.isRawTable());
  }

  //Tests for Inode methods
  @Test
  public void comparableTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    InodeFolder inode2 = new InodeFolder("test2", 2, 0);
    Assert.assertEquals(-1, inode1.compareTo(inode2));
  }

  @Test
  public void equalsTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    InodeFolder inode2 = new InodeFolder("test2", 1, 0);
    Assert.assertTrue(inode1.equals(inode2));
  }

  @Test
  public void isDirectoryTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertTrue(inode1.isDirectory());
  }

  @Test
  public void isFileTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertFalse(inode1.isFile());
  }

  @Test
  public void getInodeTypeTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertEquals(InodeType.Folder, inode1.getInodeType());
  }

  @Test
  public void getIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertEquals(1, inode1.getId());
  }

  @Test
  public void reverseIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    inode1.reverseId();
    Assert.assertEquals(-1, inode1.getId());
  }

  @Test
  public void setNameTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  @Test
  public void setParentIdTest() {
    InodeFolder inode1 = new InodeFolder("test1", 1, 0);
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  } 
}
