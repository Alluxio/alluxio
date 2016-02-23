/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.Constants;
import alluxio.master.block.BlockId;
import alluxio.security.authorization.PermissionStatus;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Abstract class for serving inode tests.
 */
public abstract class AbstractInodeTest {
  public static final String TEST_USER_NAME = "user1";
  public static final String TEST_GROUP_NAME = "group1";

  private static PermissionStatus sPermissionStatus =
      new PermissionStatus(TEST_USER_NAME, TEST_GROUP_NAME, (short)0755);
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  protected long createInodeFileId(long containerId) {
    return BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
  }

  protected static InodeDirectory createInodeDirectory() {
    return new InodeDirectory.Builder().setName("test1").setId(1).setParentId(0)
        .setPermissionStatus(sPermissionStatus).build();
  }

  protected InodeFile createInodeFile(long id) {
    return new InodeFile.Builder().setName("testFile" + id).setBlockContainerId(id).setParentId(1)
        .setBlockSizeBytes(Constants.KB).setPermissionStatus(sPermissionStatus).build();
  }
}
