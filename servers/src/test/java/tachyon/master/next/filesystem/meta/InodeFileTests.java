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

package tachyon.master.next.filesystem.meta;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.master.block.BlockId;

/**
 * Unit tests for tachyon.InodeFile
 */
public final class InodeFileTests {
  @Test
  public void equalsTest() {
    InodeFile inode1 = createInodeFile(1);
    // self equal
    Assert.assertEquals(inode1, inode1);
    InodeFile inode2 = new InodeFile("test2", 1, 0, 1000, System.currentTimeMillis());
    // equal with same id
    Assert.assertEquals(inode1, inode2);
    InodeFile inode3 = createInodeFile(3);
    Assert.assertFalse(inode1.equals(inode3));
  }

  @Test
  public void getIdTest() {
    InodeFile inode1 = createInodeFile(1);
    Assert.assertEquals(createBlockId(1), inode1.getId());
  }

  private long createBlockId(long containerId) {
    return BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
  }

  private static InodeFile createInodeFile(long id) {
    return new InodeFile("testFile" + id, id, 1, Constants.KB, System.currentTimeMillis());
  }
}
