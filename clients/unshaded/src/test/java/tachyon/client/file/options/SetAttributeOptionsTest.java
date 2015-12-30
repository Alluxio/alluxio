package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link SetAttributeOptions} class.
 */
public class SetAttributeOptionsTest {
  @Test
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    Assert.assertFalse(options.hasPersisted());
    Assert.assertFalse(options.hasPinned());
    Assert.assertFalse(options.hasTTL());
  }
}
