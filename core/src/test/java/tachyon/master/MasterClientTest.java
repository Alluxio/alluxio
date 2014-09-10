package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NoWorkerException;

/**
 * Unit tests for tachyon.MasterClient
 */
public class MasterClientTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @Test
  public void openCloseTest() throws FileAlreadyExistException, InvalidPathException, TException,
      IOException {
    MasterClient masterClient = new MasterClient(mMasterInfo.getMasterAddress());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.user_createFile("/file", "", Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
    masterClient.close();
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
  }

  @Test(timeout = 3000, expected = FileNotFoundException.class)
  public void user_getClientBlockInfoReturnsOnError() throws TException, IOException {
    // this test was created to show that a infi loop happens
    // the timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    MasterClient masterClient = new MasterClient(mMasterInfo.getMasterAddress());
    masterClient.user_getClientBlockInfo(Long.MAX_VALUE);
  }

  @Test(timeout = 3000, expected = NoWorkerException.class)
  public void user_getWorkerReturnsWhenNotLocal() throws TException, IOException {
    // this test was created to show that a infi loop happens
    // the timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    MasterClient masterClient = new MasterClient(mMasterInfo.getMasterAddress());
    masterClient.user_getWorker(false, "host.doesnotexist.fail");
  }
}
