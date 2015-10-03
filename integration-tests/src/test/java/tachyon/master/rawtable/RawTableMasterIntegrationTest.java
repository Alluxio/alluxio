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

package tachyon.master.rawtable;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TableColumnException;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.file.FileSystemMaster;

public class RawTableMasterIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonConf mMasterConf;
  private RawTableMaster mRawTableMaster;
  private FileSystemMaster mFsMaster;

  @Rule
  public final ExpectedException mException = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMasterConf = mLocalTachyonCluster.getMasterTachyonConf();
    mRawTableMaster = mLocalTachyonCluster.getMaster().getInternalMaster().getRawTableMaster();
    mFsMaster = mLocalTachyonCluster.getMaster().getInternalMaster().getFileSystemMaster();
  }

  @Ignore
  @Test
  public void createRawTableTest() throws InvalidPathException, FileAlreadyExistsException,
      TableColumnException, FileDoesNotExistException, TachyonException, IOException {
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), 1, null);
    Assert.assertTrue(
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testTable"))).isFolder);
  }

  @Test
  public void negativeColumnTest() throws InvalidPathException, FileAlreadyExistsException,
      TableColumnException, TachyonException, IOException {
    mException.expect(TableColumnException.class);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), -1, null);
  }

  @Test
  public void tooManyColumnsTest() throws InvalidPathException, FileAlreadyExistsException,
      TableColumnException, TachyonException, IOException {
    mException.expect(TableColumnException.class);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"),
        mMasterConf.getInt(Constants.MAX_COLUMNS) + 1, null);
  }
}
