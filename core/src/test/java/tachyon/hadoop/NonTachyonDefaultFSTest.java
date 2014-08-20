package tachyon.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.master.LocalTachyonCluster;

/**
 * These test cases test Tachyon when Apache Hadoop's {@code fs.defaultFS} points to a non-tachyon
 * {@link org.apache.hadoop.fs.FileSystem}.
 */
public final class NonTachyonDefaultFSTest {
  private static final Logger LOG = Logger.getLogger(NonTachyonDefaultFSTest.class);

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
    FileSystem fs = path.getFileSystem(CONF);
    Assert.assertFalse(fs instanceof AbstractTFS);

    Path tachyonPath = inTachyon(path);
    FileSystem tachyonFs = tachyonPath.getFileSystem(CONF);
    Assert.assertTrue(tachyonFs instanceof AbstractTFS);
    Assert.assertFalse(tachyonFs.exists(tachyonPath));
  }

  @Test
  public void exists() throws IOException {
    Path path = new Path("/path/in/ufs/" + System.nanoTime());
    FileSystem fs = path.getFileSystem(CONF);
    Assert.assertFalse(fs instanceof AbstractTFS);

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
    String uri = CLUSTER.getTempFolderInUnderFs();
    if (URI.create(uri).getScheme() == null) {
      uri = new File(uri).getAbsoluteFile().toURI().toString();
    }
    LOG.info("Setting Hadoop's default FS to underfs: " + uri);
    conf.set("fs.defaultFS", uri);
    // need to set in 1.x... YAY!
    conf.set("fs.default.name", uri);
    return conf;
  }
}
