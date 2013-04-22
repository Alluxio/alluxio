package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * Unit tests for tachyon.client.RawTable.
 */
public class RawTableTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonClient mClient = null;

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
  public void rawtablePerfTest() {
    int col = 100;
//    int fileId = mClient.createRawTable("/table", col);
//    RawTable table = mClient.getRawTable(fileId);
//    Assert.assertEquals(k, table.getColumns());
//    table = mClient.getRawTable("/table" + k);
//    Assert.assertEquals(k, table.getColumns());
//
//    fileId = mClient.createRawTable("/tabl" + k, k, TestUtils.getIncreasingByteBuffer(k % 10));
//    table = mClient.getRawTable(fileId);
//    Assert.assertEquals(k, table.getColumns());
//    table = mClient.getRawTable("/tabl" + k);
//    Assert.assertEquals(k, table.getColumns());
//
//    val rawTable: RawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(path)
//        val buffers = Array.tabulate[ByteBuffer](rawTable.getColumns()) { columnIndex =>
//        val fp = rawTable.getRawColumn(columnIndex).getPartition(theSplit.index)
//        var buf: ByteBuffer = fp.readByteBuffer()
//        if (buf == null && fp.recacheData()) {
//          buf = fp.readByteBuffer()
//        }
//        if (buf == null) {
//          // TODO Log that this is not good.
//          buf = ByteBuffer.allocate(fp.length().toInt)
//              val is = fp.createInStream(OpType.READ_TRY_CACHE)
//              is.read(buf.array)
//              is.close()
//              buf.limit(fp.length().toInt)
//        }
    }

    @Test
    public void getColumnsTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException, FileDoesNotExistException {
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
    public void getIdTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException {
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
    public void getNameTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException {
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
    public void getPathTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException {
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
    public void getMetadataTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException {
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
    public void updateMetadataTest()
        throws InvalidPathException, FileAlreadyExistException, TableColumnException,
        TableDoesNotExistException, TException {
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
