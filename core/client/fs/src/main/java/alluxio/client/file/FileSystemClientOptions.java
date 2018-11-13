/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
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
import alluxio.client.WriteType;
import alluxio.grpc.*;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;

public final class FileSystemClientOptions {

  public static FileSystemMasterCommonPOptions getCommonOptions() {
    return FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.NO_TTL)
        .setTtlAction(alluxio.grpc.TtlAction.DELETE)
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .build();
  }

  public static GetStatusPOptions getGetStatusOptions() {
    return GetStatusPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  public static ExistsPOptions getExistsOptions() {
    return ExistsPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  public static ListStatusPOptions getListStatusOptions() {
    FileSystemMasterCommonPOptions commonOptions =
        getCommonOptions().toBuilder().setTtl(Configuration.getMs(PropertyKey.USER_FILE_LOAD_TTL))
            .setTtlAction(GrpcUtils.toProto(
                Configuration.getEnum(PropertyKey.USER_FILE_LOAD_TTL_ACTION, TtlAction.class)))
            .build();

    return ListStatusPOptions.newBuilder().setCommonOptions(commonOptions)
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  public static CreateFilePOptions getCreateFileOptions() {
    // TODO(ggezer) WritePType conversion logic
    return CreateFilePOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(true)
        .setBlockSizeBytes(Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT))
        .setFileWriteLocationPolicy(Configuration.get(PropertyKey.USER_FILE_WRITE_LOCATION_POLICY))
        .setWriteTier(Configuration.getInt(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT))
        .setWriteType(WritePType.valueOf("WRITE_" + Configuration.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT)))
        .setPersisted(Configuration
            .getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).isThrough())
        .setMode(ModeUtils.applyFileUMask(Mode.defaults()).toShort())
        .setReplicationDurable(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE))
        .setReplicationMin(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN))
        .setReplicationMax(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX)).build();
  }

  public static OpenFilePOptions getOpenFileOptions() {
    // TODO(ggezer) ReadPType conversion logic
    return OpenFilePOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setReadType(
            ReadPType.valueOf("READ_" + Configuration.get(PropertyKey.USER_FILE_READ_TYPE_DEFAULT)))
        .setFileReadLocationPolicy(
            Configuration.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setHashingNumberOfShards(Configuration
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS))
        .setMaxUfsReadConcurrency(
            Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX))
        .build();
  }


  public static CreateDirectoryPOptions getCreateDirectoryOptions() {
    // TODO(ggezer) WritePType conversion logic
    return CreateDirectoryPOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(false)
            .setWriteType(WritePType.valueOf("WRITE_" + Configuration.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT)))
            .setPersisted(Configuration
                    .getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).isThrough())
            .setMode(ModeUtils.applyFileUMask(Mode.defaults()).toShort())
            .setAllowExist(false).build();
  }

  public static LoadMetadataPOptions getLoadMetadataOptions() {
    return LoadMetadataPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setRecursive(false).build();
  }

  public static DeletePOptions getDeleteOptions() {
    return DeletePOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(false)
        .setAlluxioOnly(false)
        .setUnchecked(Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED)).build();
  }

  public static FreePOptions getFreeOptions() {
    return FreePOptions.newBuilder().setCommonOptions(getCommonOptions()).setForced(false)
        .setRecursive(false).build();
  }

  public static MountPOptions getMountOptions() {
    return MountPOptions.newBuilder().setCommonOptions(getCommonOptions()).setReadOnly(false)
        .setShared(false).build();
  }

  public static UnmountPOptions getUnmountOptions() {
    return UnmountPOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  public static SetAclPOptions getSetAclOptions() {
    return SetAclPOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }
}
