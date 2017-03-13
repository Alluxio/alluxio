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
import alluxio.master.block.BlockId;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.security.authorization.Mode;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Abstract class for serving inode tests.
 */
public abstract class AbstractInodeTest {
  public static final String TEST_OWNER = "user1";
  public static final String TEST_GROUP = "group1";
  public static final Mode TEST_DIR_MODE = new Mode((short) 0755);
  public static final Mode TEST_FILE_MODE = new Mode((short) 0644);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * @param containerId the container id to create the InodeFile id with
   * @return the InodeFile id constructed with the container id
   */
  protected long createInodeFileId(long containerId) {
    return BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
  }

  /**
   * @return the {@link InodeDirectory} representation
   */
  protected static InodeDirectory createInodeDirectory() {
    return InodeDirectory.create(1, 0, "test1",
        CreateDirectoryOptions.defaults().setOwner(TEST_OWNER).setGroup(TEST_GROUP)
            .setMode(TEST_DIR_MODE));
  }

  /**
   * @param id block container id of this inode
   * @return the {@link InodeFile} representation
   */
  protected InodeFile createInodeFile(long id) {
    return InodeFile.create(id, 1, "testFile" + id, 0,
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(TEST_OWNER)
            .setGroup(TEST_GROUP).setMode(TEST_FILE_MODE));
  }
}
