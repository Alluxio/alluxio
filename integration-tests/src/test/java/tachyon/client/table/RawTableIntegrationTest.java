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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.thrift.RawTableInfo;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for tachyon.client.RawTable.
 */
// TODO(calvin): Move this to TachyonRawTablesIntegrationTest
public class RawTableIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource = new LocalTachyonClusterResource(
      10000, 1000, Constants.GB, Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100));
  private TachyonRawTables mTachyonRawTables = null;
  private int mMaxCols = 1000;

  @Before
  public final void before() throws Exception {
    mTachyonRawTables = TachyonRawTables.TachyonRawTablesFactory.get();
    mMaxCols =
        mLocalTachyonClusterResource.get().getMasterTachyonConf().getInt(Constants.MAX_COLUMNS);
  }

  @Test
  public void getColumnsTest() throws Exception {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/table" + k);
      RawTable table = mTachyonRawTables.create(uri, k);
      RawTableInfo info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(k, info.getColumns());

      uri = new TachyonURI("/table2" + k);
      table = mTachyonRawTables.create(uri, k, BufferUtils.getIncreasingByteBuffer(k % 10));
      info = mTachyonRawTables.getInfo(table);
      Assert.assertEquals(k, table.getColumns());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(k, table.getColumns());
    }
  }

  @Test
  public void getIdTest() throws IOException {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/table" + k);
      long fileId = mTachyonRawTables.createRawTable(uri, 1);
      RawTable table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(fileId, table.getId());

      uri = new TachyonURI("/tabl" + k);
      fileId = mTachyonRawTables.createRawTable(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(fileId, table.getId());
    }
  }

  @Test
  public void getMetadataTest() throws IOException {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      long fileId = mTachyonRawTables.createRawTable(uri, 1);
      RawTable table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTachyonRawTables.createRawTable(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 7));
      table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
    }
  }

  @Test
  public void getNameTest() throws IOException {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      long fileId = mTachyonRawTables.createRawTable(uri, 1);
      RawTable table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals("table" + k, table.getName());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals("table" + k, table.getName());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTachyonRawTables.createRawTable(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals("tab" + k, table.getName());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals("tab" + k, table.getName());
    }
  }

  @Test
  public void getPathTest() throws IOException {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      long fileId = mTachyonRawTables.createRawTable(uri, 1);
      RawTable table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals("/x/table" + k, table.getPath());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals("/x/table" + k, table.getPath());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTachyonRawTables.createRawTable(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 10));
      table = mTachyonRawTables.getRawTable(fileId);
      Assert.assertEquals("/y/tab" + k, table.getPath());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals("/y/tab" + k, table.getPath());
    }
  }

  @Test
  public void rawtablePerfTest() throws IOException {
    int col = 200;

    TachyonURI uri = new TachyonURI("/table");
    long fileId = mTachyonRawTables.createRawTable(uri, col);

    RawTable table = mTachyonRawTables.getRawTable(fileId);
    Assert.assertEquals(col, table.getColumns());
    table = mTachyonRawTables.getRawTable(uri);
    Assert.assertEquals(col, table.getColumns());

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      rawCol.createPartition(0);
      TachyonFile file = rawCol.getPartition(0);
      FileOutStream outStream = file.getOutStream();
      outStream.write(BufferUtils.getIncreasingByteArray(10));
      outStream.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      FileInStream is = file.getInStream(ReadType.CACHE);
      ByteBuffer buf = ByteBuffer.allocate(10);
      is.read(buf.array());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(10), buf);
      is.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      FileInStream is = file.getInStream(ReadType.CACHE);
      ByteBuffer buf = ByteBuffer.allocate(10);
      is.read(buf.array());
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(10), buf);
      is.close();
    }
  }

  @Test
  public void updateMetadataTest() throws IOException {
    for (int k = 1; k < mMaxCols; k += mMaxCols / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      long fileId = mTachyonRawTables.createRawTable(uri, 1);
      RawTable table = mTachyonRawTables.getRawTable(fileId);
      table.updateMetadata(BufferUtils.getIncreasingByteBuffer(k % 17));
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTachyonRawTables.createRawTable(uri, 1, BufferUtils.getIncreasingByteBuffer(k % 7));
      table = mTachyonRawTables.getRawTable(fileId);
      table.updateMetadata(BufferUtils.getIncreasingByteBuffer(k % 16));
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
      table = mTachyonRawTables.getRawTable(uri);
      Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
    }
  }
}
