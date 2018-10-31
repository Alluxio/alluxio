package alluxio.master.file;

import alluxio.grpc.*;
import alluxio.wire.LoadMetadataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link alluxio.master.file.FileSystemMasterOptions}.
 */
public class FileSystemMasterOptionsTest {

  private FileSystemMasterOptions mMasterOptions;

  @Before
  public void init() {
    mMasterOptions = new DefaultFileSystemMasterOptions();
  }

  @Test
  public void listStatusOptionsDefaults() {
    ListStatusPOptions options = mMasterOptions.getListStatusOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
    Assert.assertEquals(false, options.getRecursive());
  }

  @Test
  public void loadMetadataOptionsDefaults() {
    LoadMetadataPOptions options = mMasterOptions.getLoadMetadataOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getCreateAncestors());
    Assert.assertEquals(options.getLoadDescendantType(), LoadDescendantPType.NONE);
  }

  @Test
  public void deleteOptionsDefaults() {
    DeletePOptions options = mMasterOptions.getDeleteOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getAlluxioOnly());
    Assert.assertFalse(options.getUnchecked());
  }
}
