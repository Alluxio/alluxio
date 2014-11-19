package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.thrift.ClientFileInfo;
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
  public void openCloseTest() throws TException,
      IOException {
    MasterClient masterClient = null;
    try {
      masterClient = new MasterClient(mMasterInfo.getMasterAddress());
      Assert.assertFalse(masterClient.isConnected());
      try {
        masterClient.connect();
        Assert.assertTrue(masterClient.isConnected());
        masterClient.user_createFile("/file", "", Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
        Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
      } finally {
        masterClient.close();
        Assert.assertFalse(masterClient.isConnected());
      }
      try {
        masterClient.connect();
        Assert.assertTrue(masterClient.isConnected());
        Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
      } finally {
        masterClient.close();
      }

    } finally {
      masterClient.close();
      Assert.assertFalse(masterClient.isConnected());
    }
  }

  @Test
  public void createDeleteTest() throws Exception {
    MasterClient masterClient = null;
    try {
      masterClient = new MasterClient(mMasterInfo.getMasterAddress());
      Assert.assertFalse(masterClient.isConnected());
      try {
        masterClient.connect();
        Assert.assertTrue(masterClient.isConnected());
        masterClient.user_createFile("/file", "", Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
        masterClient.user_delete(-1, "/file", false);
        ClientFileInfo fileStatus = masterClient.getFileStatus(-1, "/file");
        Assert.assertTrue(fileStatus != null);
        Assert.assertEquals(false, fileStatus.isCache);
        Assert.assertEquals(0, fileStatus.getInMemoryPercentage());
        Assert.assertNull(fileStatus.getBlockIds());
        Assert.assertNull(fileStatus.getName());
        Assert.assertNull(fileStatus.getPath());
      } finally {
        masterClient.close();
        Assert.assertFalse(masterClient.isConnected());
      }
      try {
        masterClient.connect();
        Assert.assertTrue(masterClient.isConnected());
        Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
      } finally {
        masterClient.close();
        Assert.assertFalse(masterClient.isConnected());
      }
    } finally {
      masterClient.close();
      Assert.assertFalse(masterClient.isConnected());
    }
  }

  @Test
  public void deleteNonExistentTest()
      throws TException,
      IOException {
    MasterClient masterClient = null;
    try {
      masterClient = new MasterClient(mMasterInfo.getMasterAddress());
      Assert.assertFalse(masterClient.isConnected());
      masterClient.connect();
      Assert.assertTrue(masterClient.isConnected());
      masterClient.user_delete(-1, "/neverexisted", false);
      ClientFileInfo fileStatus = masterClient.getFileStatus(-1, "/neverexisted");
      //If a file does not exist, tachyon fs returns null
      Assert.assertTrue(fileStatus != null);
      Assert.assertEquals(false, fileStatus.isCache);
      Assert.assertEquals(0, fileStatus.getInMemoryPercentage());
      Assert.assertNull(fileStatus.getBlockIds());
      Assert.assertNull(fileStatus.getName());
      Assert.assertNull(fileStatus.getPath());
    } finally {
      masterClient.close();
      Assert.assertFalse(masterClient.isConnected());
    }
  }

  @Test(timeout = 3000, expected = FileNotFoundException.class)
  public void user_getClientBlockInfoReturnsOnError() throws TException, IOException {
    MasterClient masterClient = null;
    try {
      // this test was created to show that a infi loop happens
      // the timeout will protect against this, and the change was to throw a IOException
      // in the cases we don't want to disconnect from master
      masterClient = new MasterClient(mMasterInfo.getMasterAddress());
      masterClient.user_getClientBlockInfo(Long.MAX_VALUE);
    } finally {
      masterClient.close();
      Assert.assertFalse(masterClient.isConnected());
    }
  }

  @Test(timeout = 3000, expected = NoWorkerException.class)
  public void user_getWorkerReturnsWhenNotLocal() throws TException, IOException {
    // this test was created to show that a infi loop happens
    // the timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    MasterClient masterClient = null;
    try {
      masterClient = new MasterClient(mMasterInfo.getMasterAddress());
      masterClient.user_getWorker(false, "host.doesnotexist.fail");
    } finally {
      masterClient.close();
      Assert.assertFalse(masterClient.isConnected());
    }
  }
}
