package alluxio.client.util;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.util.FileSystemOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileSystemOptionsTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() throws Exception {
    mConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void loadMetadataOptionsDefaults() {
    LoadMetadataPOptions options = FileSystemOptions.loadMetadataDefaults(mConf);
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getCreateAncestors());
    Assert.assertEquals(options.getLoadDescendantType(), LoadDescendantPType.NONE);
  }

  @Test
  public void deleteOptionsDefaults() {
    DeletePOptions options = FileSystemOptions.deleteDefaults(mConf);
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getAlluxioOnly());
    Assert.assertFalse(options.getUnchecked());
  }

  @Test
  public void mountOptionsDefaults() {
    MountPOptions options = FileSystemOptions.mountDefaults(mConf);
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getShared());
    Assert.assertFalse(options.getReadOnly());
    Assert.assertEquals(0, options.getPropertiesMap().size());
  }

  @Test
  public void setAclOptionsDefaults() {
    SetAclPOptions options = FileSystemOptions.setAclDefaults(mConf);
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
  }
}
