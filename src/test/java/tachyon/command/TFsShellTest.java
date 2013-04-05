package tachyon.command;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
  LocalTachyonCluster mLocalTachyonCluster = null;
  TachyonClient mClient = null;
  TFsShell mFsShell = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(5555, 6666, 1000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
    mFsShell = new TFsShell();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void mkdirTest() throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "tachyon://" + 
        InetAddress.getLocalHost().getCanonicalHostName() + ":5555/root/testFile1"});
    Assert.assertTrue(mClient.getFile("/root/testFile1") != null);
  }
}
