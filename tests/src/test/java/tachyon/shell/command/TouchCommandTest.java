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

import tachyon.TachyonURI;
import tachyon.client.file.URIStatus;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for touch command.
 */
public class TouchCommandTest extends AbstractTfsShellTest {
  @Test
  public void touchTest() throws IOException, TachyonException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.run(argv);
    URIStatus status = mFileSystem.getStatus(new TachyonURI("/testFile"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }

  @Test
  public void touchTestWithFullURI() throws IOException, TachyonException {
    String tachyonURI = "tachyon://" + mLocalTachyonCluster.getMasterHostname() + ":"
        + mLocalTachyonCluster.getMasterPort() + "/destFileURI";
    // when
    String[] argv = new String[] {"touch", tachyonURI};
    mFsShell.run(argv);
    // then
    URIStatus status = mFileSystem.getStatus(new TachyonURI("/destFileURI"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }
}
