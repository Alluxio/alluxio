package tachyon;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.client.TachyonFS;

/**
 * Local Tachyon cluster with multiple master for unit tests.
 */
public class MasterFaultToleranceTest {  
  private final int BLOCK_SIZE = 30;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.default.block.size.byte", String.valueOf(BLOCK_SIZE));
    mLocalTachyonClusterMultiMaster = new LocalTachyonClusterMultiMaster(10000, 5);
    mLocalTachyonClusterMultiMaster.start();
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonClusterMultiMaster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
  }

  @Test
  public void emptyTest() {
  }

  @Test
  public void getClientsTest() throws IOException {
    int clients = 10;
    mTfs.createFile("/0", 1024);
    for (int k = 1; k < clients; k ++) {
      TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
      tfs.createFile("/" + k, 1024);
    }
    List<String> files = mTfs.ls("/", true);
    Assert.assertEquals(clients + 1, files.size());
    Assert.assertEquals("/", files.get(0));
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals("/" + k, files.get(k + 1));
    }
  }

  @Test
  public void faultTest() throws IOException {
    int clients = 10;
    mTfs.createFile("/0", 1024);
    for (int k = 1; k < clients; k ++) {
      TachyonFS tfs = mLocalTachyonClusterMultiMaster.getClient();
      tfs.createFile("/" + k, 1024);
    }
    List<String> files = mTfs.ls("/", true);
    Assert.assertEquals(clients + 1, files.size());
    Assert.assertEquals("/", files.get(0));
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals("/" + k, files.get(k + 1));
    }

    Assert.assertTrue(mLocalTachyonClusterMultiMaster.killLeader());
    CommonUtils.sleepMs(null, 2000);

    mTfs = mLocalTachyonClusterMultiMaster.getClient();
    files = mTfs.ls("/", true);
    for (int k = 0; k < files.size(); k ++) {
      System.out.println(k + " " + files.get(k));
    }
    Assert.assertEquals(clients + 1, files.size());
    Assert.assertEquals("/", files.get(0));
    for (int k = 0; k < clients; k ++) {
      Assert.assertEquals("/" + k, files.get(k + 1));
    }
  }
}
