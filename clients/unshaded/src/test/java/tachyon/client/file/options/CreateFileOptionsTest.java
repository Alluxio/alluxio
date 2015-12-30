package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;
import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.WriteType;
import tachyon.thrift.CreateTOptions;

/**
 * Tests for the {@link CreateFileOptions} class.
 */
public class CreateFileOptionsTest {
  private final long mDefaultBlockSizeBytes = ClientContext.getConf().getBytes(
      Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private final WriteType mDefaultWriteType = ClientContext.getConf().getEnum(
      Constants.USER_FILE_WRITE_TYPE_DEFAULT, tachyon.client.WriteType.class);

  // TODO(calvin): Test location policy when a factory is created
  @Test
  public void defaultsTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(mDefaultBlockSizeBytes, options.getBlockSizeBytes());
    Assert.assertEquals(mDefaultWriteType.getTachyonStorageType(), options.getTachyonStorageType());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTTL());
  }

  @Test
  public void toThriftTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    CreateTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(), thriftOptions
        .isPersisted());
    Assert.assertEquals(mDefaultBlockSizeBytes, thriftOptions.getBlockSizeBytes());
    Assert.assertEquals(Constants.NO_TTL, thriftOptions.getTtl());
  }
}
