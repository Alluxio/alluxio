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
import tachyon.client.WriteType;
import tachyon.exception.TachyonException;
import tachyon.security.LoginUser;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for chgrp command.
 */
public class ChgrpCommandTest extends AbstractTfsShellTest {
  @Test
  public void chgrpTest() throws IOException, TachyonException {
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chgrp", "group1", "/testFile");
    String group = mTfs.getStatus(new TachyonURI("/testFile")).getGroupName();
    Assert.assertEquals("group1", group);
    mFsShell.run("chgrp", "group2", "/testFile");
    group = mTfs.getStatus(new TachyonURI("/testFile")).getGroupName();
    Assert.assertEquals("group2", group);
  }
}
