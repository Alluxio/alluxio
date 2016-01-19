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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.exception.TableColumnException;
import tachyon.exception.TableMetadataException;
import tachyon.master.file.FileSystemMaster;

public class RawTableMasterIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB);
  private TachyonConf mMasterConf;
  private RawTableMaster mRawTableMaster;
  private FileSystemMaster mFsMaster;

  @Rule
  public final ExpectedException mException = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    mMasterConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mRawTableMaster =
        mLocalTachyonClusterResource.get().getMaster().getInternalMaster().getRawTableMaster();
    mFsMaster =
        mLocalTachyonClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster();
  }

  @Test
  public void createRawTableTest() throws Exception {
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), 1, ByteBuffer.allocate(0));
    Assert.assertTrue(
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testTable"))).isIsFolder());
  }

  @Test
  public void negativeColumnTest() throws Exception {
    mException.expect(TableColumnException.class);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), -1, null);
  }

  @Test
  public void tooManyColumnsTest() throws Exception {
    mException.expect(TableColumnException.class);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"),
        mMasterConf.getInt(Constants.MAX_COLUMNS) + 1, null);
  }

  @Test
  public void nullMetadataTest() throws Exception {
    mException.expect(NullPointerException.class);
    mException.expectMessage(PreconditionMessage.RAW_TABLE_METADATA_NULL);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), 1, null);
  }

  @Test
  public void tooBigMetadataTest() throws Exception {
    mException.expect(TableMetadataException.class);
    mRawTableMaster.createRawTable(new TachyonURI("/testTable"), 1,
        ByteBuffer.allocate((int) mMasterConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE) + 1));
  }
}
