package alluxio.client.file.options;

import alluxio.CommonTestUtils;
import alluxio.thrift.ListStatusTOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ListStatusOptions} class.
 */
public class ListStatusOptionsTest {
  @Test
  public void defaultsTest() {
    ListStatusOptions options = ListStatusOptions.defaults();

    Assert.assertTrue(options.isLoadDirectChildren());
  }

  @Test
  public void fieldsTest() {
    ListStatusOptions options = ListStatusOptions.defaults();
    Assert.assertEquals(true, options.isLoadDirectChildren());
  }

  @Test
  public void toThriftTest() {
    ListStatusOptions options = ListStatusOptions.defaults();
    ListStatusTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(true, options.isLoadDirectChildren());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(ListStatusOptions.class);
  }
}
