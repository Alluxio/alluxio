package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ExistsOptions} class.
 */
public class ExistsOptionsTest {
  @Test
  public void defaultsTest() {
    ExistsOptions options = ExistsOptions.defaults();
    Assert.assertFalse(options.isCheckUfs());
  }
}
