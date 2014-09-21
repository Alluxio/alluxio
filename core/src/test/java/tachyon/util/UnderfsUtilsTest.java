package tachyon.util;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.PrefixList;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemCluster;
import tachyon.client.TachyonFS;
import tachyon.master.LocalTachyonCluster;

/**
 * To test the utilities related to under filesystem, including loadufs and etc.
 */
public class UnderfsUtilsTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private String mUnderfsAddress = null;
  private UnderFileSystem mUfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.default.block.size.byte", "128");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();

    mTfs = mLocalTachyonCluster.getClient();

    mUnderfsAddress = System.getProperty("tachyon.underfs.address");
    mUfs = UnderFileSystem.get(mUnderfsAddress + TachyonURI.SEPARATOR);
  }

  @Test
  public void loadUnderFsTest() throws IOException {
    if (!UnderFileSystemCluster.isUFSHDFS()) {
      return;
    }

    String[] exclusions = {"/tachyon", "/exclusions"};
    String[] inclusions = {"/inclusions/sub-1", "/inclusions/sub-2"};
    for (String exclusion : exclusions) {
      if (!mUfs.exists(exclusion)) {
        mUfs.mkdirs(exclusion, true);
      }
    }

    for (String inclusion : inclusions) {
      if (!mUfs.exists(inclusion)) {
        mUfs.mkdirs(inclusion, true);
      }
      CommonUtils.touch(mUnderfsAddress + inclusion + "/1");
    }

    UfsUtils.loadUnderFs(mTfs, new TachyonURI(TachyonURI.SEPARATOR), new TachyonURI(mUnderfsAddress
        + TachyonURI.SEPARATOR), new PrefixList("tachyon;exclusions", ";"));

    List<String> paths = null;
    for (String exclusion : exclusions) {
      try {
        paths = TestUtils.listFiles(mTfs, exclusion);
        fail("NO FileDoesNotExistException is expected here");
      } catch (IOException ioe) {
        Assert.assertNotNull(ioe);
      }
      Assert.assertNull("Not exclude the target folder: " + exclusion, paths);
    }

    for (String inclusion : inclusions) {
      paths = TestUtils.listFiles(mTfs, inclusion);
      Assert.assertNotNull(paths);
    }
  }
}
