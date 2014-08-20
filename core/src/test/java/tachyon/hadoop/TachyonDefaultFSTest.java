package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tachyon.master.LocalTachyonCluster;

/**
 * These test cases test Tachyon when Apache Hadoop's {@code fs.defaultFS} points to tachyon
 * {@link org.apache.hadoop.fs.FileSystem}.
 * <p />
 * When this class runs under the local profile, then a infinite recursion happens on the
 * {@code FileSystem.get(...);}.  This is caused by Tachyon not using {@code file:} for
 * underfs.  So when the underfs is a file path, hadoop assumes that the path is on
 * the default FS, which means tachyon will check underfs which causes tachyon to
 * check the underfs...
 */
@Ignore
public final class TachyonDefaultFSTest {

  private LocalTachyonCluster CLUSTER = null;
  private Configuration CONF = null;

  @Before
  public void before() {
    CLUSTER = LocalTachyonCluster.createAndStartOrThrow(1000);
    CONF = createConfig();
  }

  @After
  public void after() throws Exception {
    CLUSTER.stop();
  }

  @Test
  public void notExists() throws IOException {
    Path path = new Path("/foo/bar/baz");

    Path tachyonPath = inTachyon(path);
    FileSystem tachyonFs = tachyonPath.getFileSystem(CONF);
    Assert.assertTrue(tachyonFs instanceof AbstractTFS);
    Assert.assertFalse(tachyonFs.exists(tachyonPath));
  }

  @Test
  public void exists() throws IOException {
    Path path = new Path("/path/in/ufs/" + System.nanoTime());

    Path tachyonPath = inTachyon(path);
    FileSystem tachyonFs = tachyonPath.getFileSystem(CONF);
    Assert.assertTrue(tachyonFs instanceof AbstractTFS);
    Assert.assertFalse(tachyonFs.exists(tachyonPath));
    HadoopUtils.createByteFile(tachyonFs, tachyonPath, 10);
    Assert.assertTrue(tachyonFs.exists(tachyonPath));
  }

  private Path inTachyon(final Path path) {
    return new Path(CLUSTER.getUri(), path);
  }

  private Configuration createConfig() {
    final Configuration conf = HadoopUtils.createConfig();
    conf.set("fs.defaultFS", CLUSTER.getUri());
    // need to set in 1.x... YAY!
    conf.set("fs.default.name", CLUSTER.getUri());
    return conf;
  }
}
