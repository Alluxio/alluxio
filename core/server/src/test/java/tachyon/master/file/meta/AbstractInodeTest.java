/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.meta;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.master.block.BlockId;
import tachyon.security.authorization.PermissionStatus;

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
