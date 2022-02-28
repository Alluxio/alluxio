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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link MutableInodeDirectory}.
 */
public final class MutableInodeDirectoryTest extends AbstractInodeTest {
  private static final Logger LOG = LoggerFactory.getLogger(MutableInodeDirectoryTest.class);

  /**
   * Tests the {@link MutableInodeDirectory#equals(Object)} method.
   */
  @Test
  public void equalsTest() throws Exception {
    MutableInodeDirectory inode1 =
        MutableInodeDirectory.create(1, 0, "test1", CreateDirectoryContext.defaults());
    MutableInodeDirectory inode2 =
        MutableInodeDirectory.create(1, 0, "test2", CreateDirectoryContext.defaults());
    MutableInodeDirectory inode3 =
        MutableInodeDirectory.create(3, 0, "test3", CreateDirectoryContext.defaults());
    Assert.assertTrue(inode1.equals(inode2));
    Assert.assertTrue(inode1.equals(inode1));
    Assert.assertFalse(inode1.equals(inode3));
  }

  /**
   * Tests the {@link MutableInodeDirectory#getId()} method.
   */
  @Test
  public void getId() {
    Assert.assertEquals(1, createInodeDirectory().getId());
  }

  /**
   * Tests the {@link MutableInodeDirectory#isDirectory()} method.
   */
  @Test
  public void isDirectory() {
    Assert.assertTrue(createInodeDirectory().isDirectory());
  }

  /**
   * Tests the {@link MutableInodeDirectory#isFile()} method.
   */
  @Test
  public void isFile() {
    Assert.assertFalse(createInodeDirectory().isFile());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setDeleted(boolean)} method.
   */
  @Test
  public void deleteInode() {
    MutableInodeDirectory inode1 = createInodeDirectory();
    Assert.assertFalse(inode1.isDeleted());
    inode1.setDeleted(true);
    Assert.assertTrue(inode1.isDeleted());
    inode1.setDeleted(false);
    Assert.assertFalse(inode1.isDeleted());
  }

  /**
   * Tests the last modification time is initially set to the creation time.
   */
  @Test
  public void initialLastModificationTime() {
    long lowerBoundMs = System.currentTimeMillis();
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long upperBoundMs = System.currentTimeMillis();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    Assert.assertTrue(lowerBoundMs <= lastModificationTimeMs);
    Assert.assertTrue(upperBoundMs >= lastModificationTimeMs);
  }

  /**
   * Tests the last modification time is initially set to the creation time.
   */
  @Test
  public void initialLastAccessTime() {
    long lowerBoundMs = System.currentTimeMillis();
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long upperBoundMs = System.currentTimeMillis();
    long lastAccessTimeMs = inodeDirectory.getLastAccessTimeMs();
    Assert.assertTrue(lowerBoundMs <= lastAccessTimeMs);
    Assert.assertTrue(upperBoundMs >= lastAccessTimeMs);
  }

  /**
   * Tests the {@link MutableInodeDirectory#setLastModificationTimeMs(long)} method.
   */
  @Test
  public void setLastModificationTime() {
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    long newLastModificationTimeMs = lastModificationTimeMs + Constants.SECOND_MS;
    inodeDirectory.setLastModificationTimeMs(newLastModificationTimeMs);
    Assert.assertEquals(newLastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setLastModificationTimeMs(long)} method when setting an
   * invalid time.
   */
  @Test
  public void setInvalidLastModificationTime() {
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long lastModificationTimeMs = inodeDirectory.getLastModificationTimeMs();
    long invalidModificationTimeMs = lastModificationTimeMs - Constants.SECOND_MS;
    inodeDirectory.setLastModificationTimeMs(invalidModificationTimeMs);
    Assert.assertEquals(lastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setLastAccessTimeMs(long, boolean)} method.
   */
  @Test
  public void setLastAccessTime() {
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long lastAccessTimeMs = inodeDirectory.getLastAccessTimeMs();
    long newLastAccessTimeMs = lastAccessTimeMs + Constants.SECOND_MS;
    inodeDirectory.setLastAccessTimeMs(newLastAccessTimeMs);
    Assert.assertEquals(newLastAccessTimeMs, inodeDirectory.getLastAccessTimeMs());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setLastAccessTimeMs(long, boolean)} method.
   */
  @Test
  public void setInvalidLastAccessTime() {
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
    long lastAccessTimeMs = inodeDirectory.getLastAccessTimeMs();
    long invalidAccessTimeMs = lastAccessTimeMs - Constants.SECOND_MS;
    inodeDirectory.setLastAccessTimeMs(invalidAccessTimeMs);
    Assert.assertEquals(lastAccessTimeMs, inodeDirectory.getLastAccessTimeMs());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setName(String)} method.
   */
  @Test
  public void setName() {
    MutableInodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    Assert.assertEquals("test2", inode1.getName());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setParentId(long)} method.
   */
  @Test
  public void setParentId() {
    MutableInodeDirectory inode1 = createInodeDirectory();
    Assert.assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    Assert.assertEquals(2, inode1.getParentId());
  }

  /**
   * Tests the {@link MutableInodeDirectory#getMode()} method.
   */
  @Test
  public void permissionStatus() {
    MutableInodeDirectory inode2 = createInodeDirectory();
    Assert.assertEquals(TEST_OWNER, inode2.getOwner());
    Assert.assertEquals(TEST_GROUP, inode2.getGroup());
    Assert.assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults(),
        ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK)).toShort(),
        inode2.getMode());
  }

  /**
   * Tests the {@link MutableInodeDirectory#generateClientFileInfo(String)} method.
   */
  @Test
  public void generateClientFileInfo() {
    MutableInodeDirectory inodeDirectory = createInodeDirectory();
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
