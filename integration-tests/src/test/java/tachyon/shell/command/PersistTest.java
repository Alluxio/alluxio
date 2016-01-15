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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Test used to persist once
 */
public class PersistTest extends AbstractTfsShellTest {
  @Test
  public void persistTest() throws IOException, TachyonException {
    String testFilePath = "/testPersist/testFile";
    TachyonFile testFile =
        TachyonFSTestUtils.createByteFile(mTfs, testFilePath, TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, 10);
    Assert.assertFalse(mTfs.getInfo(testFile).isIsPersisted());

    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n", mOutput.toString());
    checkFilePersisted(testFile, 10);
  }
}
