/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.Constants;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link InodeDirectory}.
 */
public final class InodeDirectoryTest extends AbstractInodeTest {
  private static final Logger LOG = LoggerFactory.getLogger(InodeDirectoryTest.class);

  /**
   * Tests the {@link InodeDirectory#addChild(Inode)} method.
   */
  @Test
  public void addChildren() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(2);
    InodeFile inodeFile2 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile2);
    Assert.assertEquals(Sets.newHashSet(createInodeFileId(2), createInodeFileId(3)),
        inodeDirectory.getChildrenIds());
  }

  /**
   * Tests the {@link InodeDirectory#removeChild(String)} method after multiple children have been
   * added.
   */
  @Test
  public void batchRemoveChild() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    InodeFile inodeFile2 = createInodeFile(2);
    InodeFile inodeFile3 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile2);
    inodeDirectory.addChild(inodeFile3);
    Assert.assertEquals(3, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild("testFile1");
    Assert.assertEquals(2, inodeDirectory.getNumberOfChildren());
    Assert.assertFalse(inodeDirectory.getChildrenIds().contains(createInodeFileId(1)));
  }

  /**
   * Tests the {@link InodeDirectory#equals(Object)} method.
   */
  @Test
  public void equalsTest() throws Exception {
    InodeDirectory inode1 = InodeDirectory.create(1, 0, "test1", CreateDirectoryOptions.defaults());
    InodeDirectory inode2 = InodeDirectory.create(1, 0, "test2", CreateDirectoryOptions.defaults());
    InodeDirectory inode3 = InodeDirectory.create(3, 0, "test3", CreateDirectoryOptions.defaults());
    Assert.assertTrue(inode1.equals(inode2));
    Assert.assertTrue(inode1.equals(inode1));
    Assert.assertFalse(inode1.equals(inode3));
  }

  /**
   * Tests the {@link InodeDirectory#getId()} method.
   */
  @Test
  public void getId() {
    Assert.assertEquals(1, createInodeDirectory().getId());
  }

  /**
   * Tests the {@link InodeDirectory#isDirectory()} method.
   */
  @Test
  public void isDirectory() {
    Assert.assertTrue(createInodeDirectory().isDirectory());
  }

  /**
   * Tests the {@link InodeDirectory#isFile()} method.
   */
  @Test
  public void isFile() {
    Assert.assertFalse(createInodeDirectory().isFile());
  }

  /**
   * Tests the {@link InodeDirectory#removeChild(Inode)} method.
   */
  @Test
  public void removeChild() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild(inodeFile1);
    Assert.assertEquals(0, inodeDirectory.getNumberOfChildren());
  }

  /**
   * Tests the {@link InodeDirectory#removeChild(Inode)} method with a non-existent child.
   */
  @Test
  public void removeNonExistentChild() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(2);
    InodeFile inodeFile2 = createInodeFile(3);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
    inodeDirectory.removeChild(inodeFile2);
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
  }

  /**
   * Tests the {@link InodeDirectory#setDeleted(boolean)} method.
   */
  @Test
  public void deleteInode() {
    InodeDirectory inode1 = createInodeDirectory();
    Assert.assertFalse(inode1.isDeleted());
    inode1.setDeleted(true);
    Assert.assertTrue(inode1.isDeleted());
    inode1.setDeleted(false);
    Assert.assertFalse(inode1.isDeleted());
  }

  /**
   * Tests that the {@link InodeDirectory#addChild(Inode)} method only created one child with the
   * same id.
   */
  @Test
  public void sameIdChildren() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    InodeFile inodeFile1 = createInodeFile(1);
    inodeDirectory.addChild(inodeFile1);
    inodeDirectory.addChild(inodeFile1);
    Assert.assertTrue(inodeDirectory.getChildrenIds().contains(createInodeFileId(1)));
    Assert.assertEquals(1, inodeDirectory.getNumberOfChildren());
  }

  /**
   * Tests the last modification time is initially set to the creation time.
   */
  @Test
  public void initialLastModificationTime() {
    long lowerBoundMs = System.currentTimeMillis();
    InodeDirectory inodeDirectory = createInodeDirectory();
    long upperBoundMs = System.currentTimeMillis();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    Assert.assertTrue(lowerBoundMs <= lastModificationTimeMs);
    Assert.assertTrue(upperBoundMs >= lastModificationTimeMs);
  }

  /**
   * Tests the {@link InodeDirectory#setLastModificationTimeMs(long)} method.
   */
  @Test
  public void setLastModificationTime() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    long newLastModificationTimeMs = lastModificationTimeMs + Constants.SECOND_MS;
    inodeDirectory.setLastModificationTimeMs(newLastModificationTimeMs);
    Assert.assertEquals(newLastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
  }

  /**
   * Tests the {@link InodeDirectory#setLastModificationTimeMs(long)} method when setting an
   * invalid time.
   */
  @Test
  public void setInvalidLastModificationTime() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    long invalidModificationTimeMs = lastModificationTimeMs - Constants.SECOND_MS;
    inodeDirectory.setLastModificationTimeMs(invalidModificationTimeMs);
    Assert.assertEquals(lastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
  }

  /**
   * Tests the {@link InodeDirectory#setName(String)} method.
   */
  @Test
  public void setName() {
    InodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  /**
   * Tests the {@link InodeDirectory#setParentId(long)} method.
   */
  @Test
  public void setParentId() {
    InodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }

  /**
   * Tests the {@link InodeDirectory#getChild(String)} methods.
   */
  @Test
  public void getChild() {
    // large number of small files
    InodeDirectory inodeDirectory = createInodeDirectory();
    int nFiles = (int) 1E5;
    Inode<?>[] inodes = new Inode[nFiles];
    for (int i = 0; i < nFiles; i++) {
      inodes[i] = createInodeFile(i + 1);
      inodeDirectory.addChild(inodes[i]);
    }

    Runtime runtime = Runtime.getRuntime();
    LOG.info(String.format("Used Memory = %dB when number of files = %d",
        runtime.totalMemory() - runtime.freeMemory(), nFiles));

    long start = System.currentTimeMillis();
    for (int i = 0; i < nFiles; i++) {
      Assert.assertEquals(inodes[i], inodeDirectory.getChild(String.format("testFile%d", i + 1)));
    }
    LOG.info(String.format("getChild(String name) called sequentially %d times, cost %d ms", nFiles,
        System.currentTimeMillis() - start));
  }

  /**
   * Tests the {@link InodeDirectory#getMode()} method.
   */
  @Test
  public void permissionStatus() {
    InodeDirectory inode2 = createInodeDirectory();
    Assert.assertEquals(TEST_OWNER, inode2.getOwner());
    Assert.assertEquals(TEST_GROUP, inode2.getGroup());
    Assert.assertEquals(Mode.defaults().applyDirectoryUMask().toShort(), inode2.getMode());
  }

  /**
   * Tests the {@link InodeDirectory#generateClientFileInfo(String)} method.
   */
  @Test
  public void generateClientFileInfo() {
    InodeDirectory inodeDirectory = createInodeDirectory();
    String path = "/test/path";
    FileInfo info = inodeDirectory.generateClientFileInfo(path);
    Assert.assertEquals(inodeDirectory.getId(), info.getFileId());
    Assert.assertEquals(inodeDirectory.getName(), info.getName());
    Assert.assertEquals(path, info.getPath());
    Assert.assertEquals("", info.getUfsPath());
    Assert.assertEquals(0, info.getLength());
    Assert.assertEquals(0, info.getBlockSizeBytes());
    Assert.assertEquals(inodeDirectory.getCreationTimeMs(), info.getCreationTimeMs());
    Assert.assertTrue(info.isCompleted());
    Assert.assertTrue(info.isFolder());
    Assert.assertEquals(inodeDirectory.isPinned(), info.isPinned());
    Assert.assertFalse(info.isCacheable());
    Assert.assertNotNull(info.getBlockIds());
    Assert.assertEquals(inodeDirectory.getLastModificationTimeMs(),
        info.getLastModificationTimeMs());
  }
}
