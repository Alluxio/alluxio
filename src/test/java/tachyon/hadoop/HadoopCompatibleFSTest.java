package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.client.TachyonFS;

/**
 * Unit tests for <code>tachyon.hadoop.HadoopCompatibleFS</code>.
 */
public class HadoopCompatibleFSTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mClient = null;

  private MiniDFSCluster mDfsCluster = null;
  private DistributedFileSystem mDfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();

    Configuration conf = new Configuration();
    mDfsCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = mDfsCluster.getFileSystem();
    Assert.assertTrue("Not a HDFS: "+fs.getUri(), fs instanceof DistributedFileSystem);
    mDfs = (DistributedFileSystem) fs;
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void test() {
  }
}
