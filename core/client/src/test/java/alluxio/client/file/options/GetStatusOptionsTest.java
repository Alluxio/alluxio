package alluxio.client.file.options;

import alluxio.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link GetStatusOptions}.
 */
public class GetStatusOptionsTest {
  @Test
  public void defaultsTest() {
    GetStatusOptions options = GetStatusOptions.defaults();

    Assert.assertNotNull(options);
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(GetStatusOptions.class);
  }
}
