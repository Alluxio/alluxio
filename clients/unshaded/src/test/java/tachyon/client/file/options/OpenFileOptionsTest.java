package tachyon.client.file.options;

import org.junit.Assert;
import org.junit.Test;
import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;

/**
 * Tests for the {@link OpenFileOptions} class.
 */
public class OpenFileOptionsTest {
  private final ReadType mDefaultReadType =
      ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);

  @Test
  public void defaultsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    Assert.assertEquals(mDefaultReadType.getTachyonStorageType(), options.getTachyonStorageType());
  }

  @Test
  public void toInStreamOptionsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    Assert.assertEquals(mDefaultReadType.getTachyonStorageType(),
        inStreamOptions.getTachyonStorageType());
  }
}
