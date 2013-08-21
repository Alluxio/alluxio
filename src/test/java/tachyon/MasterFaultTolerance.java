package tachyon;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.client.TachyonFS;

/**
 * Local Tachyon cluster with multiple master for unit tests.
 */
public class MasterFaultTolerance {  
  private final int BLOCK_SIZE = 30;

  private LocalTachyonClusterMultiMaster mLocalTachyonClusterMultiMaster = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.default.block.size.byte", String.valueOf(BLOCK_SIZE));
    System.out.println("Good 0");
    mLocalTachyonClusterMultiMaster = new LocalTachyonClusterMultiMaster(10000, 2);
    System.out.println("Good 1");
    mLocalTachyonClusterMultiMaster.start();
    System.out.println("Good A");
    mTfs = mLocalTachyonClusterMultiMaster.getClient();
    System.out.println("Good B");
  }

  @After
  public final void after() throws Exception {
    System.out.println("Good C");
    mLocalTachyonClusterMultiMaster.stop();
    System.out.println("Good D");
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
  }

  @Test
  public void simpleTest() {
  }
}
