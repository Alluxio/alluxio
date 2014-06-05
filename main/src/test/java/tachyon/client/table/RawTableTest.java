/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.client.table.RawColumn;
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for tachyon.client.RawTable.
 */
public class RawTableTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void getColumnsTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/table" + k, k);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mTfs.getRawTable("/table" + k);
      Assert.assertEquals(k, table.getColumns());

      fileId = mTfs.createRawTable("/tabl" + k, k, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mTfs.getRawTable("/tabl" + k);
      Assert.assertEquals(k, table.getColumns());
    }
  }

  @Test
  public void getIdTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/table" + k, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTfs.getRawTable("/table" + k);
      Assert.assertEquals(fileId, table.getId());

      fileId = mTfs.createRawTable("/tabl" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTfs.getRawTable("/tabl" + k);
      Assert.assertEquals(fileId, table.getId());
    }
  }

  @Test
  public void getMetadataTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/x/table" + k, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      table = mTfs.getRawTable("/x/table" + k);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

      fileId = mTfs.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      table = mTfs.getRawTable("/y/tab" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
    }
  }

  @Test
  public void getNameTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/x/table" + k, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals("table" + k, table.getName());
      table = mTfs.getRawTable("/x/table" + k);
      Assert.assertEquals("table" + k, table.getName());

      fileId = mTfs.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals("tab" + k, table.getName());
      table = mTfs.getRawTable("/y/tab" + k);
      Assert.assertEquals("tab" + k, table.getName());
    }
  }

  @Test
  public void getPathTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/x/table" + k, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals("/x/table" + k, table.getPath());
      table = mTfs.getRawTable("/x/table" + k);
      Assert.assertEquals("/x/table" + k, table.getPath());

      fileId = mTfs.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals("/y/tab" + k, table.getPath());
      table = mTfs.getRawTable("/y/tab" + k);
      Assert.assertEquals("/y/tab" + k, table.getPath());
    }
  }

  @Test
  public void rawtablePerfTest() throws IOException {
    int col = 200;

    long sMs = System.currentTimeMillis();
    int fileId = mTfs.createRawTable("/table", col);
    // System.out.println("A " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    RawTable table = mTfs.getRawTable(fileId);
    Assert.assertEquals(col, table.getColumns());
    table = mTfs.getRawTable("/table");
    Assert.assertEquals(col, table.getColumns());
    // System.out.println("B " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      rawCol.createPartition(0);
      TachyonFile file = rawCol.getPartition(0);
      OutStream outStream = file.getOutStream(WriteType.MUST_CACHE);
      outStream.write(TestUtils.getIncreasingByteArray(10));
      outStream.close();
    }
    // System.out.println("C " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      TachyonByteBuffer buf = file.readByteBuffer();
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), buf.DATA);
      buf.close();
    }
    // System.out.println("D " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      TachyonByteBuffer buf = file.readByteBuffer();
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), buf.DATA);
      buf.close();
    }
    // System.out.println("E " + (System.currentTimeMillis() - sMs));
  }

  @Test
  public void updateMetadataTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      int fileId = mTfs.createRawTable("/x/table" + k, 1);
      RawTable table = mTfs.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 17));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());
      table = mTfs.getRawTable("/x/table" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());

      fileId = mTfs.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mTfs.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 16));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
      table = mTfs.getRawTable("/y/tab" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
    }
  }
}
