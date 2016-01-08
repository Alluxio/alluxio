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

package tachyon.client.table;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.thrift.RawTableInfo;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link RawTable}.
 */
// TODO(calvin): Move this to TachyonRawTablesIntegrationTest
public class RawTableIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource = new LocalTachyonClusterResource(
      10 * Constants.MB, 1000, Constants.MB, Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100));
  private TachyonFileSystem mTachyonFileSystem = null;
  private TachyonRawTables mTachyonRawTables = null;
  private int mMaxCols = 1000;

  @Before
  public final void before() throws Exception {
    mTachyonFileSystem = TachyonFileSystem.TachyonFileSystemFactory.get();
    mTachyonRawTables = TachyonRawTables.TachyonRawTablesFactory.get();
    mMaxCols =
        mLocalTachyonClusterResource.get().getMasterTachyonConf().getInt(Constants.MAX_COLUMNS);
  }

  @Test
  @Ignore("TACHYON-1278")
  // This test often fails because it takes more than a minute to complete the table creation call.
  // We are tracking this issue in TACHYON-1278
  public void getColumnsTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/table" + k);
      RawTable table = mTachyonRawTables.create(uri, k);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(k, info.getColumns());

      uri = new TachyonURI("/tabl" + k);
      table = mTachyonRawTables.create(uri, k, BufferUtils.getIncreasingByteBuffer(k % 10));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(k, info.getColumns());
    }
  }

  @Test
  public void getIdTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/table" + k);
      RawTable table = mTachyonRawTables.create(uri, 1);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(table.getRawTableId(), info.getId());

      uri = new TachyonURI("/tabl" + k);
      table = mTachyonRawTables.create(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(table.getRawTableId(), info.getId());
    }
  }

  @Test
  public void getMetadataTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      RawTable table = mTachyonRawTables.create(uri, 1);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertArrayEquals(new byte[0], info.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      table = mTachyonRawTables.create(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 7));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteBuffer(k % 7).array(),
          info.getMetadata());
    }
  }

  @Test
  public void getNameTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      RawTable table = mTachyonRawTables.create(uri, 1);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals("table" + k, info.getName());

      uri = new TachyonURI("/y/tab" + k);
      table = mTachyonRawTables.create(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals("tab" + k, info.getName());
    }
  }

  @Test
  public void getPathTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      RawTable table = mTachyonRawTables.create(uri, 1);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals("/x/table" + k, info.getPath());

      uri = new TachyonURI("/y/tab" + k);
      table = mTachyonRawTables.create(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals("/y/tab" + k, info.getPath());
    }
  }

  @Test
  public void rawtablePerfTest() throws Exception {
    int col = 200;

    TachyonURI uri = new TachyonURI("/table");
    RawTable table = mTachyonRawTables.create(uri, col);
    RawTableInfo info = mTachyonRawTables.getInfo(table);

    Assert.assertEquals(col, info.getColumns());

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getColumn(k);
      FileOutStream outStream = mTachyonRawTables.createPartition(rawCol, 0);
      outStream.write(BufferUtils.getIncreasingByteArray(10));
      outStream.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getColumn(k);
      TachyonFile file = mTachyonRawTables.openPartition(rawCol, 0);
      FileInStream is = mTachyonFileSystem.getInStream(file);
      ByteBuffer buf = ByteBuffer.allocate(10);
      is.read(buf.array());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(10), buf);
      is.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getColumn(k);
      TachyonFile file = mTachyonRawTables.openPartition(rawCol, 0);
      FileInStream is = mTachyonFileSystem.getInStream(file);
      ByteBuffer buf = ByteBuffer.allocate(10);
      is.read(buf.array());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(10), buf);
      is.close();
    }
  }

  @Test
  public void updateMetadataTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      RawTable table = mTachyonRawTables.create(uri, 1);
      mTachyonRawTables.updateRawTableMetadata(table, BufferUtils.getIncreasingByteBuffer(k % 17));
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteBuffer(k % 17).array(),
          info.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      table = mTachyonRawTables.create(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 7));
      mTachyonRawTables.updateRawTableMetadata(table, BufferUtils.getIncreasingByteBuffer(k % 16));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteBuffer(k % 16).array(),
          info.getMetadata());
    }
  }
}
