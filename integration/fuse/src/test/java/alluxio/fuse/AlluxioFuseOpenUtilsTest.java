package alluxio.fuse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AlluxioFuseOpenUtils}.
 */
public class AlluxioFuseOpenUtilsTest {
  @Test
  public void readOnly() {
    int[] readFlags = new int[]{0x8000, 0x9000};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void writeOnly() {
    // 0x8001 0x9001 O_WDONLY
    int[] readFlags = new int[]{0x8001, 0x9001};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.WRITE_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void readWrite() {
    int[] readFlags = new int[]{0x8002, 0x9002, 0xc002};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_WRITE,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }
}
