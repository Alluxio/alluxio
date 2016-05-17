package alluxio.client.file.options;

import org.junit.Assert;
import org.junit.Test;

import alluxio.CommonTestUtils;

/**
 * Tests for the {@link ExistsOptions} class.
 */
public class ExistsOptionsTest {
  @Test
  public void defaultsTest() {
    ExistsOptions options = ExistsOptions.defaults();

    Assert.assertNotNull(options);
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(ExistsOptions.class);
  }
}
