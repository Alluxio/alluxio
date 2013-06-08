package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;

/**
 * Unit tests for tachyon.client.RawTable.
 */
public class RawTableTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mClient = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void rawtablePerfTest() throws IOException {
    int col = 200;

    long sMs = System.currentTimeMillis();
    int fileId = mClient.createRawTable("/table", col);
    //    System.out.println("A " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    RawTable table = mClient.getRawTable(fileId);
    Assert.assertEquals(col, table.getColumns());
    table = mClient.getRawTable("/table");
    Assert.assertEquals(col, table.getColumns());
    //    System.out.println("B " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      rawCol.createPartition(0);
      TachyonFile file = rawCol.getPartition(0);
      OutStream outStream = file.getOutStream(OpType.WRITE_CACHE);
      outStream.write(TestUtils.getIncreasingByteArray(10));
      outStream.close();
    }
    //    System.out.println("C " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), file.readByteBuffer());
      file.releaseFileLock();
    }
    //    System.out.println("D " + (System.currentTimeMillis() - sMs));

    sMs = System.currentTimeMillis();
    for (int k = 0; k < col; k ++) {
      RawColumn rawCol = table.getRawColumn(k);
      TachyonFile file = rawCol.getPartition(0, true);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), file.readByteBuffer());
      file.releaseFileLock();
    }
    //    System.out.println("E " + (System.currentTimeMillis() - sMs));
  }

  @Test
  public void getColumnsTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/table" + k, k);
      RawTable table = mClient.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mClient.getRawTable("/table" + k);
      Assert.assertEquals(k, table.getColumns());

      fileId = mClient.createRawTable("/tabl" + k, k, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mClient.getRawTable(fileId);
      Assert.assertEquals(k, table.getColumns());
      table = mClient.getRawTable("/tabl" + k);
      Assert.assertEquals(k, table.getColumns());
    }
  }

  @Test
  public void getIdTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/table" + k, 1);
      RawTable table = mClient.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mClient.getRawTable("/table" + k);
      Assert.assertEquals(fileId, table.getId());

      fileId = mClient.createRawTable("/tabl" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mClient.getRawTable(fileId);
      Assert.assertEquals(fileId, table.getId());
      table = mClient.getRawTable("/tabl" + k);
      Assert.assertEquals(fileId, table.getId());
    }
  }

  @Test
  public void getNameTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/x/table" + k, 1);
      RawTable table = mClient.getRawTable(fileId);
      Assert.assertEquals("table" + k, table.getName());
      table = mClient.getRawTable("/x/table" + k);
      Assert.assertEquals("table" + k, table.getName());

      fileId = mClient.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mClient.getRawTable(fileId);
      Assert.assertEquals("tab" + k, table.getName());
      table = mClient.getRawTable("/y/tab" + k);
      Assert.assertEquals("tab" + k, table.getName());
    }
  }

  @Test
  public void getPathTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/x/table" + k, 1);
      RawTable table = mClient.getRawTable(fileId);
      Assert.assertEquals("/x/table" + k, table.getPath());
      table = mClient.getRawTable("/x/table" + k);
      Assert.assertEquals("/x/table" + k, table.getPath());

      fileId = mClient.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 10));
      table = mClient.getRawTable(fileId);
      Assert.assertEquals("/y/tab" + k, table.getPath());
      table = mClient.getRawTable("/y/tab" + k);
      Assert.assertEquals("/y/tab" + k, table.getPath());
    }
  }

  @Test
  public void getMetadataTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/x/table" + k, 1);
      RawTable table = mClient.getRawTable(fileId);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
      table = mClient.getRawTable("/x/table" + k);
      Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

      fileId = mClient.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mClient.getRawTable(fileId);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      table = mClient.getRawTable("/y/tab" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 7), table.getMetadata());
    }
  }

  @Test
  public void updateMetadataTest() throws IOException {
    for (int k = 1; k < Constants.MAX_COLUMNS; k += Constants.MAX_COLUMNS / 5) {
      int fileId = mClient.createRawTable("/x/table" + k, 1);
      RawTable table = mClient.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 17));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());
      table = mClient.getRawTable("/x/table" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 17), table.getMetadata());

      fileId = mClient.createRawTable("/y/tab" + k, 1, TestUtils.getIncreasingByteBuffer(k % 7));
      table = mClient.getRawTable(fileId);
      table.updateMetadata(TestUtils.getIncreasingByteBuffer(k % 16));
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
      table = mClient.getRawTable("/y/tab" + k);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(k % 16), table.getMetadata());
    }
  }
}
