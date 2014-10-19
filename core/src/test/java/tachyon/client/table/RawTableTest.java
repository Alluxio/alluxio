package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
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
      TachyonURI uri = new TachyonURI("/table" + k);
      int fileId = mTfs.createRawTable(uri, k);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(k, table.getColumns());

      uri = new TachyonURI("/tabl" + k);
      fileId = mTfs.createRawTable(uri, k, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(k, table.getColumns());
    }
  }

  @Test
  public void getIdTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      TachyonURI uri = new TachyonURI("/table" + k);
      int fileId = mTfs.createRawTable(uri, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(fileId, table.getId());

      uri = new TachyonURI("/tabl" + k);
      fileId = mTfs.createRawTable(uri, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(fileId, table.getId());
    }
  }

  @Test
  public void getMetadataTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      int fileId = mTfs.createRawTable(uri, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTfs.createRawTable(uri, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
    }
  }

  @Test
  public void getNameTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      int fileId = mTfs.createRawTable(uri, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals("table" + k, table.getName());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals("table" + k, table.getName());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTfs.createRawTable(uri, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals("tab" + k, table.getName());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals("tab" + k, table.getName());
    }
  }

  @Test
  public void getPathTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      int fileId = mTfs.createRawTable(uri, 1);
      RawTable table = mTfs.getRawTable(fileId);
      Assert.assertEquals("/x/table" + k, table.getPath());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals("/x/table" + k, table.getPath());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTfs.createRawTable(uri, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mTfs.getRawTable(fileId);
      Assert.assertEquals("/y/tab" + k, table.getPath());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals("/y/tab" + k, table.getPath());
    }
  }

  @Test
  public void rawtablePerfTest() throws IOException {
    int col = 200;

    TachyonURI uri = new TachyonURI("/table");
    int fileId = mTfs.createRawTable(uri, col);

    RawTable table = mTfs.getRawTable(fileId);
    Assert.assertEquals(col, table.getColumns());
    table = mTfs.getRawTable(uri);
    Assert.assertEquals(col, table.getColumns());

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      rawCol.createPartition(0);
      TachyonFile file = rawCol.getPartition(0);
      OutStream outStream = file.getOutStream(WriteType.MUST_CACHE);
      outStream.write(TestUtils.getIncreasingByteArray(10));
      outStream.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      TachyonByteBuffer buf = file.readByteBuffer(0);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), buf.mData);
      buf.close();
    }

    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      TachyonByteBuffer buf = file.readByteBuffer(0);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), buf.mData);
      buf.close();
    }
  }

  @Test
  public void updateMetadataTest() throws IOException {
    for (int k = 1; k < CommonConf.get().MAX_COLUMNS; k += CommonConf.get().MAX_COLUMNS / 5) {
      TachyonURI uri = new TachyonURI("/x/table" + k);
      int fileId = mTfs.createRawTable(uri, 1);
      RawTable table = mTfs.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 17));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());

      uri = new TachyonURI("/y/tab" + k);
      fileId = mTfs.createRawTable(uri, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mTfs.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 16));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
      table = mTfs.getRawTable(uri);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
    }
  }
}
