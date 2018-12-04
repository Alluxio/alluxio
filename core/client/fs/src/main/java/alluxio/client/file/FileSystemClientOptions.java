/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.grpc.CheckConsistencyPOptions;
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
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UfsPMode;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;
import alluxio.grpc.TtlAction;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The file system class to set default options for client.
 */
@ThreadSafe
public final class FileSystemClientOptions {

  /**
   * @return {@link FileSystemMasterCommonPOptions} instance with default values for client
   */
  public static FileSystemMasterCommonPOptions getCommonOptions() {
    return FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.NO_TTL)
        .setTtlAction(alluxio.grpc.TtlAction.DELETE)
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .build();
  }

  /**
   * @return {@link GetStatusPOptions} instance with default values for client
   */
  public static GetStatusPOptions getGetStatusOptions() {
    return GetStatusPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  /**
   * @return {@link ExistsPOptions} instance with default values for client
   */
  public static ExistsPOptions getExistsOptions() {
    return ExistsPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  /**
   * @return {@link ListStatusPOptions} instance with default values for client
   */
  public static ListStatusPOptions getListStatusOptions() {
    return ListStatusPOptions.newBuilder()
        .setCommonOptions(getCommonOptions().toBuilder()
            .setTtl(Configuration.getMs(PropertyKey.USER_FILE_LOAD_TTL))
            .setTtlAction(GrpcUtils.toProto(
                Configuration.getEnum(PropertyKey.USER_FILE_LOAD_TTL_ACTION, TtlAction.class))))
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  /**
   * @return {@link CreateFilePOptions} instance with default values for client
   */
  public static CreateFilePOptions getCreateFileOptions() {
    return CreateFilePOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(true)
        .setBlockSizeBytes(Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT))
        .setFileWriteLocationPolicy(Configuration.get(PropertyKey.USER_FILE_WRITE_LOCATION_POLICY))
        .setWriteTier(Configuration.getInt(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT))
        .setWriteType(Configuration
            .getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).toProto())
        .setMode(ModeUtils.applyFileUMask(Mode.defaults()).toShort())
        .setReplicationDurable(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE))
        .setReplicationMin(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN))
        .setReplicationMax(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX)).build();
  }

  /**
   * @return {@link OpenFilePOptions} instance with default values for client
   */
  public static OpenFilePOptions getOpenFileOptions() {
    return OpenFilePOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setReadType(Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class)
            .toProto())
        .setFileReadLocationPolicy(
            Configuration.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setHashingNumberOfShards(Configuration
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS))
        .setMaxUfsReadConcurrency(
            Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX))
        .build();
  }

  /**
   * @return {@link CreateDirectoryPOptions} instance with default values for client
   */
  public static CreateDirectoryPOptions getCreateDirectoryOptions() {
    return CreateDirectoryPOptions.newBuilder()
        .setCommonOptions(getCommonOptions().toBuilder()
            .setTtl(Configuration.getLong(PropertyKey.USER_FILE_CREATE_TTL))
            .setTtlAction(GrpcUtils.toProto(
                Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class))))
        .setRecursive(false)
        .setWriteType(Configuration
            .getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).toProto())
        .setMode(ModeUtils.applyDirectoryUMask(Mode.defaults()).toShort()).setAllowExists(false)
        .build();
  }

  /**
   * @return {@link LoadMetadataPOptions} instance with default values for client
   */
  public static LoadMetadataPOptions getLoadMetadataOptions() {
    return LoadMetadataPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setRecursive(false).build();
  }

  /**
   * @return {@link DeletePOptions} instance with default values for client
   */
  public static DeletePOptions getDeleteOptions() {
    return DeletePOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(false)
        .setAlluxioOnly(false)
        .setUnchecked(Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED)).build();
  }

  /**
   * @return {@link FreePOptions} instance with default values for client
   */
  public static FreePOptions getFreeOptions() {
    return FreePOptions.newBuilder().setCommonOptions(getCommonOptions()).setForced(false)
        .setRecursive(false).build();
  }

  /**
   * @return {@link MountPOptions} instance with default values for client
   */
  public static MountPOptions getMountOptions() {
    return MountPOptions.newBuilder().setCommonOptions(getCommonOptions()).setReadOnly(false)
        .setShared(false).build();
  }

  /**
   * @return {@link UnmountPOptions} instance with default values for client
   */
  public static UnmountPOptions getUnmountOptions() {
    return UnmountPOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  /**
   * @return {@link SetAclPOptions} instance with default values for client
   */
  public static SetAclPOptions getSetAclOptions() {
    return SetAclPOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  /**
   * @return {@link RenamePOptions} instance with default values for client
   */
  public static RenamePOptions getRenameOptions() {
    return RenamePOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  /**
   * @return {@link SetAttributePOptions} instance with default values for client
   */
  public static SetAttributePOptions getSetAttributeOptions() {
    return SetAttributePOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setRecursive(false).setTtlAction(alluxio.grpc.TtlAction.DELETE).build();
  }

  /**
   * @return {@link CheckConsistencyPOptions} instance with default values for client
   */
  public static CheckConsistencyPOptions getCheckConsistencyOptions() {
    return CheckConsistencyPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .build();
  }

  /**
   * @return {@link UpdateUfsModePOptions} instance with default values for client
   */
  public static UpdateUfsModePOptions getUpdateUfsModeOptions() {
    return UpdateUfsModePOptions.newBuilder().setUfsMode(UfsPMode.READ_WRITE).build();
  }

  /**
   * @return {@link CompleteFilePOptions} instance with default values for client
   */
  public static CompleteFilePOptions getCompleteFileOptions() {
    return CompleteFilePOptions.newBuilder().setCommonOptions(getCommonOptions()).setUfsLength(0)
        .build();
  }
}
