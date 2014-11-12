package tachyon.master;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  public void writeImageTest() {
    // create the InodeFolder and the output streams
    long creationTime = System.currentTimeMillis();
    InodeFolder inode1 = new InodeFolder("test1", 1, 0, creationTime);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    // write the image
    try {
      inode1.writeImage(writer, dos);
    } catch (IOException ioe) {
      Assert.fail("Unexpected IOException: " + ioe.getMessage());
    }

    // decode the written bytes
    ImageElement decoded = null;
    try {
      decoded = mapper.readValue(os.toByteArray(), ImageElement.class);
    } catch (Exception e) {
      Assert.fail("Unexpected " + e.getClass() + ": " + e.getMessage());
    }

    // test the decoded ImageElement
    Assert.assertEquals(creationTime, (long) decoded.getLong("creationTimeMs"));
    Assert.assertEquals(1, (int) decoded.getInt("id"));
    Assert.assertEquals("test1", decoded.getString("name"));
    Assert.assertEquals(0, (int) decoded.getInt("parentId"));
    Assert.assertEquals(new ArrayList<Integer>(), decoded.get("childrenIds", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(creationTime, (long) decoded.getLong("lastModificationTimeMs"));
  }
}
