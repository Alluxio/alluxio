package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link FileSystemClientOptions}.
 */
public class FileSystemClientOptionsTest {
  @Test
  public void commonOptionsDefaults() {
    FileSystemMasterCommonPOptions options = FileSystemClientOptions.getCommonOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(alluxio.grpc.TtlAction.DELETE, options.getTtlAction());
  }

  @Test
  public void getStatusOptionsDefaults() {
    GetStatusPOptions options = FileSystemClientOptions.getGetStatusOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
  }

  @Test
  public void listStatusOptionsDefaults() {
    ListStatusPOptions options = FileSystemClientOptions.getListStatusOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
    Assert.assertEquals(false, options.getRecursive());
  }

  @Test
  public void loadMetadataOptionsDefaults() {
    LoadMetadataPOptions options = FileSystemClientOptions.getLoadMetadataOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
  }

  @Test
  public void deleteOptionsDefaults() {
    DeletePOptions options = FileSystemClientOptions.getDeleteOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getAlluxioOnly());
    Assert.assertEquals(options.getUnchecked(),
        Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED));
  }

  @Test
  public void freeOptionsDefaults() {
    FreePOptions options = FileSystemClientOptions.getFreeOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getForced());
  }

  @Test
  public void mountOptionsDefaults() {
    MountPOptions options = FileSystemClientOptions.getMountOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getReadOnly());
    Assert.assertFalse(options.getShared());
    Assert.assertEquals(0, options.getPropertiesMap().size());
  }

  @Test
  public void setAclOptionsDefaults() {
    SetAclPOptions options = FileSystemClientOptions.getSetAclOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
  }
}