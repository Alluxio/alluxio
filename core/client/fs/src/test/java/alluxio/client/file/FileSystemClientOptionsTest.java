package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.UfsPMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;

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

  @Test
  public void existsDefaults() {
    ExistsPOptions options = FileSystemClientOptions.getExistsOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(options.getLoadMetadataType(), GrpcUtils.toProto(
        Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)));
  }

  @Test
  public void updateUfsModeDefaults() {
    UpdateUfsModePOptions options = FileSystemClientOptions.getUpdateUfsModeOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(options.getUfsMode(), UfsPMode.READ_WRITE);
  }

  @Test
  public void createFileDefaults() {
    CreateFilePOptions options = FileSystemClientOptions.getCreateFileOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(options.getCommonOptions().getTtl(),
        Configuration.getLong(PropertyKey.USER_FILE_CREATE_TTL));
    Assert.assertEquals(options.getCommonOptions().getTtlAction(),
        Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class));
    Assert.assertEquals(options.getMode(), ModeUtils.applyFileUMask(Mode.defaults()).toShort());
    Assert.assertTrue(options.getRecursive());
    Assert.assertEquals(options.getBlockSizeBytes(),
        Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT));
    Assert.assertEquals(options.getFileWriteLocationPolicy(),
        Configuration.get(PropertyKey.USER_FILE_WRITE_LOCATION_POLICY));
    Assert.assertEquals(options.getWriteTier(),
        Configuration.getInt(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT));
    Assert.assertEquals(options.getWriteType(),
        WritePType.valueOf("WRITE_" + Configuration.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT)));
    Assert.assertEquals(options.getReplicationDurable(),
        Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE));
    Assert.assertEquals(options.getReplicationMin(),
        Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN));
    Assert.assertEquals(options.getReplicationMax(),
        Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX));
  }

  @Test
  public void createDirectoryDefaults() {
    CreateDirectoryPOptions options = FileSystemClientOptions.getCreateDirectoryOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getAllowExist());
    Assert.assertFalse(options.getRecursive());
    Assert.assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults()).toShort(),
        options.getMode());
    Assert.assertEquals(WritePType.WRITE_MUST_CACHE, options.getWriteType());
  }

  @Test
  public void completeFileDefaults() {
    CompleteFilePOptions options = FileSystemClientOptions.getCompleteFileOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(0, options.getUfsLength());
  }
}
