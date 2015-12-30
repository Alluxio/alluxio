package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link GetStatusOptions} class.
 */
public class GetStatusOptionsTest {
  @Test
  public void defaultsTest() {
    GetStatusOptions options = GetStatusOptions.defaults();
    Assert.assertFalse(options.isCheckUfs());
  }
}
