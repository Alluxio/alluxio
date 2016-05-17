package alluxio.client.file.options;

import org.junit.Assert;
import org.junit.Test;

import alluxio.CommonTestUtils;

/**
 * Tests for the {@link RenameOptions} class.
 */
public class RenameOptionsTest {
  @Test
  public void defaultsTest() {
    RenameOptions options = RenameOptions.defaults();

    Assert.assertNotNull(options);
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(RenameOptions.class);
  }
}
