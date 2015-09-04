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
import tachyon.master.file.meta.InodeDirectory;
import tachyon.master.file.meta.InodeFile;

/**
 * Abstract class for serving inode tests.
 */
public abstract class AbstractInodeTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  protected long createInodeFileId(long containerId) {
    return BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
  }

  protected static InodeDirectory createInodeDirectory() {
    return new InodeDirectory("test1", 1, 0, System.currentTimeMillis());
  }

  protected InodeFile createInodeFile(long id) {
    return new InodeFile("testFile" + id, id, 1, Constants.KB, System.currentTimeMillis());
  }
}
