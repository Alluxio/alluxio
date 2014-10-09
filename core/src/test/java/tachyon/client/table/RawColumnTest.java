package tachyon.client.table;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.CommonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * Unit tests for tachyon.client.RawColumn.
 */
public class RawColumnTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void basicTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, TableDoesNotExistException, FileDoesNotExistException, IOException,
      TException {
    int fileId = mTfs.createRawTable(new TachyonURI("/table"), CommonConf.get().MAX_COLUMNS / 10);
    RawTable table = mTfs.getRawTable(fileId);

    for (int col = 0; col < CommonConf.get().MAX_COLUMNS / 10; col ++) {
      RawColumn column = table.getRawColumn(col);
      for (int pid = 0; pid < 5; pid ++) {
        Assert.assertTrue(column.createPartition(pid));
        TachyonFile file = column.getPartition(pid);
        Assert.assertEquals("/table" + TachyonURI.SEPARATOR + MasterInfo.COL + col
            + TachyonURI.SEPARATOR + pid, file.getPath());
      }
      Assert.assertEquals(5, column.partitions());
    }
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }
}
