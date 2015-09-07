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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.file.FileSystemMaster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TachyonException;

public class RawTableMasterIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonConf mMasterConf;
  private RawTableMaster mRawTableMaster;
  private FileSystemMaster mFsMaster;

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
  public void createRawTableTest() throws InvalidPathException, FileAlreadyExistException,
        TableColumnException, FileDoesNotExistException, TachyonException {
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), 1, null);
    Assert.assertTrue(
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testTable"))).isFolder);
  }

  @Test(expected = TableColumnException.class)
  public void negativeColumnTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, TachyonException {
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), -1, null);
  }


  @Test(expected = TableColumnException.class)
  public void tooManyColumnsTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, TachyonException {
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"),
        mMasterConf.getInt(Constants.MAX_COLUMNS) + 1, null);
  }
}
