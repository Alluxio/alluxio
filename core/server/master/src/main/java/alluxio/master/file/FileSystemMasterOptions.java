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

package alluxio.master.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.wire.LoadMetadataType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The file system class to set default options for master.
 */
@ThreadSafe
public final class FileSystemMasterOptions{

  /**
   * @return {@link FileSystemMasterCommonPOptions} with default values for master
   */
  private static FileSystemMasterCommonPOptions commonDefaults() {
    return FileSystemMasterCommonPOptions.newBuilder()
        .setTtl(Configuration.getLong(PropertyKey.USER_FILE_CREATE_TTL))
        .setTtlAction(
            Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class))
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .build();
  }

  /**
   * @return Master side defaults for {@link CompleteFilePOptions}
   */
  public static CompleteFilePOptions completeFileDefaults() {
    return CompleteFilePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setUfsLength(0)
        .build();
  }

  /**
   * @return Master side defaults for {@link CreateDirectoryPOptions}
   */
  public static CreateDirectoryPOptions createDirectoryDefaults() {
    return CreateDirectoryPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setMode(ModeUtils.applyDirectoryUMask(Mode.defaults()).toShort())
        .setRecursive(false)
        .setWriteType(Configuration
            .getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).toProto())
        .setAllowExists(false).build();
  }

  /**
   * @return Master side defaults for {@link CreateFilePOptions}
   */
  public static CreateFilePOptions createFileDefaults() {
    return CreateFilePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setRecursive(false)
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
   * @return Master side defaults for {@link DeletePOptions}
   */
  public static DeletePOptions deleteDefaults() {
    return DeletePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setRecursive(false)
        .setAlluxioOnly(false)
        .setUnchecked(Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED)).build();
  }

  /**
   * @return Master side defaults for {@link FreePOptions}
   */
  public static FreePOptions freeDefaults() {
    return FreePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setForced(false)
        .setRecursive(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link GetStatusPOptions}
   */
  public static GetStatusPOptions getStatusDefaults() {
    return GetStatusPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  /**
   * @return Master side defaults for {@link ListStatusPOptions}
   */
  public static ListStatusPOptions listStatusDefaults() {
    return ListStatusPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  /**
   * @return Master side defaults for {@link LoadMetadataPOptions}
   */
  public static LoadMetadataPOptions loadMetadataDefaults() {
    return LoadMetadataPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setRecursive(false)
        .setCreateAncestors(false)
        .setLoadDescendantType(LoadDescendantPType.NONE)
        .build();
  }

  /**
   * @return Master side defaults for {@link MountPOptions}
   */
  public static MountPOptions mountDefaults() {
    return MountPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setShared(false)
        .setReadOnly(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link RenamePOptions}
   */
  public static RenamePOptions renameDefaults() {
    return RenamePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .build();
  }

  /**
   * @return Master side defaults for {@link SetAttributePOptions}
   */
  public static SetAttributePOptions setAttributesDefaults() {
    return SetAttributePOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setTtlAction(TtlAction.DELETE)
        .setRecursive(false)
        .setPinned(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link SetAclPOptions}
   */
  public static SetAclPOptions setAclDefaults() {
    return SetAclPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .setRecursive(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link CheckConsistencyPOptions}
   */
  public static CheckConsistencyPOptions checkConsistencyDefaults() {
    return CheckConsistencyPOptions.newBuilder()
        .setCommonOptions(commonDefaults())
        .build();
  }
}
