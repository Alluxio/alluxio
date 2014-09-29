package tachyon.hadoop;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.UnderFileSystem;
import tachyon.conf.CommonConf;

/**
 * Unit tests for <code>tachyon.hadoop.GlusterFS</code>.
 */
public class GlusterFSTest {
  private UnderFileSystem mGfs = null;
  private String mMount = null;
  private String mVolume = null;

  @Before
  public final void before() throws IOException {
    mMount = CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS;
    mVolume = CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES;
  }

  @Test
  public void createGlusterFS() throws Exception {
    if (mMount != null && !mMount.equals("") && mVolume != null && !mVolume.equals("")) {
      mGfs = UnderFileSystem.get("glusterfs:///");
      Assert.assertTrue(mGfs.create("tachyon_test") != null);
    }
  }
}
