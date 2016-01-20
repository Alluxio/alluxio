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

package tachyon.shell.command;

import org.junit.Assert;
import org.junit.Test;

import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.client.block.TachyonBlockStore;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.TachyonURI;

/**
 * Tests the "getUsedBytes" command
 */
public class GetUsedBytesCommandTest extends AbstractTfsShellTest{
  /**
   * Tests the available memory on the cluster after adding couple of files
   */
  @Test
  public void getUsedBytesTest() throws Exception {
    String filePathA = "/testFileA";
    String filePathB = "/testFileB";
    long tachyonCapacity = TachyonBlockStore.get().getCapacityBytes();
    int fileASize = (int)(0.2 * (double)tachyonCapacity);
    int fileBSize = (int)(0.3 * (double)tachyonCapacity);

    // Ensures initial used memory is zero
    Assert.assertEquals(0, TachyonBlockStore.get().getUsedBytes());

    //Add first file with 5MB to consume 50% 0f existing memory size
    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, filePathA, TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, fileASize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathA)));
    Assert.assertEquals(fileASize,TachyonBlockStore.get().getUsedBytes());

    //Add second file to consume all of tachyon's memory.
    TachyonFile fileB = TachyonFSTestUtils.createByteFile(mTfs, filePathB, TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, fileBSize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathB)));
    Assert.assertEquals(fileASize + fileBSize ,TachyonBlockStore.get().getUsedBytes());
  }
}
