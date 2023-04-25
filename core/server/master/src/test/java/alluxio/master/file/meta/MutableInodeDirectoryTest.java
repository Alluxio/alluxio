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

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableList;
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
    assertEquals(1, createInodeDirectory().getId());
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

  @Test
  public void testProto() {
    MutableInodeDirectory inode = createInodeDirectory();
    inode.setAcl(ImmutableList.of(new AclEntry.Builder().setType(AclEntryType.MASK)
            .setActions(Mode.Bits.READ_EXECUTE).build(),
        new AclEntry.Builder().setType(AclEntryType.NAMED_USER)
            .setActions(Mode.Bits.ALL).setSubject("Subject").build(),
        new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP)
            .setActions(Mode.Bits.READ).setSubject("Other").build(),
        new AclEntry.Builder().setType(AclEntryType.OTHER)
            .setActions(Mode.Bits.NONE).setIsDefault(true).build()));
    inode.setPersistenceState(PersistenceState.PERSISTED);
    InodeMeta.Inode proto = inode.toProto();

    MutableInodeDirectory newInode = MutableInodeDirectory.fromProto(proto);
    assertEquals(inode, newInode);
    assertEquals(inode.getACL(), newInode.getACL());

    // use the deprecated proto fields
    InodeMeta.Inode.Builder builder = proto.toBuilder();
    builder.clearNewAccessAcl();
    builder.setAccessAcl(ProtoUtils.toProto(inode.getACL()));
    builder.clearPersistenceStateEnum();
    builder.setPersistenceState(inode.getPersistenceState().toString());
    builder.clearNewDefaultAcl();
    builder.setDefaultAcl(ProtoUtils.toProto(inode.getDefaultACL()));
    newInode = MutableInodeDirectory.fromProto(builder.build());
    assertEquals(inode, newInode);
    assertEquals(inode.getACL(), newInode.getACL());
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
    assertEquals(newLastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
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
    assertEquals(lastModificationTimeMs, inodeDirectory.getLastModificationTimeMs());
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
    assertEquals(newLastAccessTimeMs, inodeDirectory.getLastAccessTimeMs());
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
    assertEquals(lastAccessTimeMs, inodeDirectory.getLastAccessTimeMs());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setName(String)} method.
   */
  @Test
  public void setName() {
    MutableInodeDirectory inode1 = createInodeDirectory();
    assertEquals("test1", inode1.getName());
    inode1.setName("test2");
    assertEquals("test2", inode1.getName());
  }

  /**
   * Tests the {@link MutableInodeDirectory#setParentId(long)} method.
   */
  @Test
  public void setParentId() {
    MutableInodeDirectory inode1 = createInodeDirectory();
    assertEquals(0, inode1.getParentId());
    inode1.setParentId(2);
    assertEquals(2, inode1.getParentId());
  }

  /**
   * Tests the {@link MutableInodeDirectory#getMode()} method.
   */
  @Test
  public void permissionStatus() {
    MutableInodeDirectory inode2 = createInodeDirectory();
    assertEquals(TEST_OWNER, inode2.getOwner());
    assertEquals(TEST_GROUP, inode2.getGroup());
    assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults(),
        Configuration.getString(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK))
            .toShort(),
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
    assertEquals(inodeDirectory.getId(), info.getFileId());
    assertEquals(inodeDirectory.getName(), info.getName());
    assertEquals(path, info.getPath());
    assertEquals("", info.getUfsPath());
    assertEquals(0, info.getLength());
    assertEquals(0, info.getBlockSizeBytes());
    assertEquals(inodeDirectory.getCreationTimeMs(), info.getCreationTimeMs());
    Assert.assertTrue(info.isCompleted());
    Assert.assertTrue(info.isFolder());
    assertEquals(inodeDirectory.isPinned(), info.isPinned());
    Assert.assertFalse(info.isCacheable());
    Assert.assertNotNull(info.getBlockIds());
    assertEquals(inodeDirectory.getLastModificationTimeMs(),
        info.getLastModificationTimeMs());
  }
}
