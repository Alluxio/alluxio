package tachyon.command;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.thrift.TException;

import tachyon.LocalTachyonCluster;
import tachyon.client.TachyonClient;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  LocalTachyonCluster ltc = null;
  TachyonClient client = null;
  TFsShell tFsShell = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    ltc = new LocalTachyonCluster(5555, 6666, 1000);
    ltc.start();
    client = ltc.getClient();
    tFsShell = new TFsShell();
  }

  @After
  public final void after() throws Exception {
    ltc.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void mkdirTest() throws InvalidPathException, FileAlreadyExistException, TException {
    tFsShell.mkdir(new String[]{"mkdir", "tachyon://saasbook:5555/root/testFile1"});
    Assert.assertTrue(client.getFile("/root/testFile1") != null);
  }
}
