package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;
import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.WriteType;
import tachyon.thrift.MkdirTOptions;

/**
 * Tests for the {@link CreateDirectoryOptions} class.
 */
public class CreateDirectoryOptionsTest {
  private final WriteType mDefaultWriteType =
      ClientContext.getConf().getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);

  @Test
  public void defaultsTest() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    Assert.assertFalse(options.isAllowExists());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
  }

  @Test
  public void toThriftTest() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    MkdirTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isAllowExists());
    Assert.assertFalse(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(),
        thriftOptions.isPersisted());
  }
}
