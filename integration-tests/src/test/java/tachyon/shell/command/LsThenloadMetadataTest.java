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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.PathUtils;

/**
 * Test for ls with a wildcard argument
 */
public class LsThenloadMetadataTest extends AbstractTfsShellTest {
  @Test
  public void lsThenloadMetadataTest() throws IOException, TachyonException {
    TachyonConf conf = mLocalTachyonCluster.getMasterTachyonConf();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(ufsRoot, "dir1"), conf);
    // First run ls to create the data
    mFsShell.run("ls", "/dir1");
    Assert.assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/dir1"))).isIsPersisted());
    // Load metadata
    mFsShell.run("loadMetadata", "/dir1");
    Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage("/dir1") + "\n",
        mOutput.toString());
  }
}
