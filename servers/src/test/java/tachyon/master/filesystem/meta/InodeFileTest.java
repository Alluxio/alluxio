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

package tachyon.master.filesystem.meta;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Unit tests for tachyon.InodeFile
 */
public final class InodeFileTest extends AbstractInodeTest {
  private static final long LENGTH = 100;

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
    Assert.assertEquals(createInodeFileId(1), inode1.getId());
  }

  @Test
  public void setLengthTest() throws Exception {
    InodeFile inodeFile = createInodeFile(1);
    inodeFile.setLength(LENGTH);
    Assert.assertEquals(LENGTH, inodeFile.getLength());
  }

  @Test
  public void setNegativeLengthTest() throws Exception {
    mThrown.expect(SuspectedFileSizeException.class);
    mThrown.expectMessage("InodeFile new length " + -1 + " is negative.");

    InodeFile inodeFile = createInodeFile(1);
    inodeFile.setLength(-1);
  }

  @Test
  public void setLengthAfterCompleteTest() throws Exception {
    mThrown.expect(SuspectedFileSizeException.class);
    mThrown.expectMessage("InodeFile length was set previously.");

    InodeFile inodeFile = createInodeFile(1);
    inodeFile.setLength(LENGTH);
    inodeFile.setLength(LENGTH);
  }

  @Test
  public void getBlockSizeBytesTest() {
    InodeFile inode1 = createInodeFile(1);
    Assert.assertEquals(Constants.KB, inode1.getBlockSizeBytes());
  }

  @Test
  public void setCompleteTest() {
    InodeFile inode1 = createInodeFile(1);
    Assert.assertFalse(inode1.isComplete());

    inode1.setComplete(LENGTH);
    Assert.assertTrue(inode1.isComplete());
  }
}
