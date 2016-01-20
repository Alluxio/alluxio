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
import org.powermock.reflect.Whitebox;

import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.exception.TachyonException;
import tachyon.security.LoginUser;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for chmodr command.
 */
public class ChmodrCommandTest extends AbstractTfsShellTest {

  @Test
  public void chmodrTest() throws IOException, TachyonException {
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    mFsShell.run("mkdir", "/testFolder1");
    TachyonFSTestUtils.createByteFile(mTfs, "/testFolder1/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 10);
    mFsShell.run("chmodr", "777", "/testFolder1");
    TachyonFile tf = mTfs.open(new TachyonURI("/testFolder1"));
    int permission = mTfs.getInfo(tf).getPermission();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmodr", "755", "/testFolder1");
    permission = mTfs.getInfo(tf).getPermission();
    Assert.assertEquals((short) 0755, permission);
  }

}
