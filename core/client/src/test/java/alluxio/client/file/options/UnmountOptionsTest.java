package alluxio.client.file.options;

import alluxio.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link UnmountOptions} class.
 */
public class UnmountOptionsTest {
  @Test
  public void defaultsTest() {
    UnmountOptions options = UnmountOptions.defaults();

    Assert.assertNotNull(options);
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(UnmountOptions.class);
  }
}
