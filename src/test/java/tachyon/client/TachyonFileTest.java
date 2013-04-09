package tachyon.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

public class TachyonFileTest {
  LocalTachyonCluster mLocalTachyonCluster = null;
  TachyonClient mClient = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  /**
   * Test <code>void append(byte b)</code>.
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void appendTest1() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);

    TachyonFile file = mClient.getFile(fileId);
    file.open(OpType.WRITE_CACHE);
    for (int k = 0; k < 100; k ++) {
      file.append((byte) k);
    }
    file.close();

    file = mClient.getFile("/root/testFile1");
    file.open(OpType.READ_TRY_CACHE);
    Assert.assertTrue(TestUtils.equalIncreasingByteBuffer(100, file.readByteBuffer()));
  }
}
