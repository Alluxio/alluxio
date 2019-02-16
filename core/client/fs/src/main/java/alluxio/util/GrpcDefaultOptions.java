package alluxio.util;

import alluxio.client.ReadType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.WritePType;

/**
 * This class contains static methods which can be passed Alluxio configuration objects that
 * will populate the gRPC options objects with the proper values based on the given configuration.
 */
public class GrpcDefaultOptions {

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static CreateDirectoryPOptions getCreateDirectoryPOptions(AlluxioConfiguration conf) {
    return CreateDirectoryPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setWriteType(conf.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WritePType.class))
        // No default configuration values, will pull from gRPC defaultInstance
        // .setMode()
        // .setRecursive()
        // .setAllowExists()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static CheckConsistencyPOptions getCheckConsistencyPOptions(AlluxioConfiguration conf) {
    return CheckConsistencyPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static CreateFilePOptions getCreateFilePOptions(AlluxioConfiguration conf) {
    return CreateFilePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setBlockSizeBytes(conf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT))
        .setFileWriteLocationPolicy(conf.get(PropertyKey.USER_FILE_WRITE_LOCATION_POLICY))
        .setWriteTier(conf.getInt(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT))
        .setWriteType(conf.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WritePType.class))
        .setReplicationDurable(conf.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE))
        .setReplicationMin(conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN))
        .setReplicationMax(conf.getInt(PropertyKey.USER_FILE_REPLICATION_MAX))
        // No default configuration values, will pull from gRPC defaultInstance
        // .setRecursive()
        // .setMode()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static DeletePOptions getDeletePOptions(AlluxioConfiguration conf) {
    return DeletePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        // No default configuration values, will pull from gRPC defaultInstance
        // .setRecursive()
        // .setAlluxioOnly()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static ExistsPOptions getExistsPOptions(AlluxioConfiguration conf) {
    return ExistsPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setLoadMetadataType(conf.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE,
            LoadMetadataPType.class))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static FileSystemMasterCommonPOptions getFileSystemMasterCommonPOptions(
      AlluxioConfiguration conf) {
    return FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(conf.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .setTtl(conf.getMs(PropertyKey.USER_FILE_CREATE_TTL))
        .setTtlAction(conf.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static FreePOptions getFreePOptions(AlluxioConfiguration conf) {
    return FreePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        // No default configuration values. Will pull from gRPC defaultInstance
        // .setForced()
        // .setRecursive()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static GetStatusPOptions getGetStatusPOptions(AlluxioConfiguration conf) {
    return GetStatusPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setLoadMetadataType(conf.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE,
            LoadMetadataPType.class))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static ListStatusPOptions getListStatusPOptions(AlluxioConfiguration conf) {
    return ListStatusPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setLoadMetadataType(conf.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE,
            LoadMetadataPType.class))
        // No default configuration values. Will pull from gRPC defaultInstance
        // .setRecursive()
        // .setLoadDirectChildren()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static MountPOptions getMountPOptions(AlluxioConfiguration conf) {
    return MountPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        // No default configuration values. Will pull from gRPC defaultInstance
        // .setReadOnly()
        // .setShared()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static OpenFilePOptions getOpenFilePOptions(AlluxioConfiguration conf) {
    return OpenFilePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setReadType(conf.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class)
            .toProto())
        .setFileReadLocationPolicy(conf.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setHashingNumberOfShards(conf
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS))
        .setMaxUfsReadConcurrency(conf.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX))
        // Not needed
        // .setFileReadLocationPolicyBytes() only for protocol level
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static RenamePOptions getRenamePOptions(AlluxioConfiguration conf) {
    return RenamePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static ScheduleAsyncPersistencePOptions getScheduleAsyncPersistOptions(
      AlluxioConfiguration conf) {
    return ScheduleAsyncPersistencePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static SetAclPOptions getSetAclPOptions(AlluxioConfiguration conf) {
    return SetAclPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        // No default configuration values. Will pull from gRPC defaultInstance
        // .setRecursive()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static SetAttributePOptions getSetAttributePOptions(AlluxioConfiguration conf) {
    return SetAttributePOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .setReplicationMin(conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN))
        .setReplicationMax(conf.getInt(PropertyKey.USER_FILE_REPLICATION_MAX))
        // No default configuration values. Will pull from gRPC defaultInstance
        // .setGroup()
        // .setGroupBytes()
        // .setMode()
        // .setOwner()
        // .setPersisted()
        // .setPinned()
        // .setRecursive()
        .build();
  }

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  public static UnmountPOptions getUnmountPOptions(AlluxioConfiguration conf) {
    return UnmountPOptions.newBuilder()
        .setCommonOptions(getFileSystemMasterCommonPOptions(conf))
        .build();
  }
}
