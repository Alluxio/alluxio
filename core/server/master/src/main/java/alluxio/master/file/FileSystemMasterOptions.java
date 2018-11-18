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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.master.file.options.SyncMetadataOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The file system class to set default options for master.
 */
@ThreadSafe
public final class FileSystemMasterOptions{

  /**
   * @return {@link FileSystemMasterCommonPOptions} with default values for master
   */
  public static FileSystemMasterCommonPOptions getCommonOptions() {
    return FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.NO_TTL)
        .setTtlAction(TtlAction.DELETE)
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .build();
  }

  /**
   * @return Master side defaults for {@link CompleteFilePOptions}
   */
  public static CompleteFilePOptions getCompleteFileOptions() {
    return CompleteFilePOptions.newBuilder().setCommonOptions(getCommonOptions()).setUfsLength(0)
        .build();
  }

  /**
   * @return Master side defaults for {@link CreateDirectoryPOptions}
   */
  public static CreateDirectoryPOptions getCreateDirectoryOptions() {
    return CreateDirectoryPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setMode(ModeUtils.applyDirectoryUMask(Mode.defaults()).toShort()).setPersisted(false)
        .setRecursive(false).setAllowExist(false).build();
  }

  /**
   * @return Master side defaults for {@link CreateFilePOptions}
   */
  public static CreateFilePOptions getCreateFileOptions() {
    return CreateFilePOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setBlockSizeBytes(Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT))
        .setReplicationDurable(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE))
        .setReplicationMax(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX))
        .setReplicationMin(Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN))
        .setMode(ModeUtils.applyFileUMask(Mode.defaults()).toShort()).setPersisted(false)
        .setRecursive(false).build();
  }

  /**
   * @return Master side defaults for {@link DeletePOptions}
   */
  public static DeletePOptions getDeleteOptions() {
    return DeletePOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(false)
        .setAlluxioOnly(false).setUnchecked(false).build();
  }

  /**
   * @return Master side defaults for {@link FreePOptions}
   */
  public static FreePOptions getFreeOptions() {
    return FreePOptions.newBuilder()
        .setCommonOptions(getCommonOptions())
        .setForced(false)
        .setRecursive(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link GetStatusPOptions}
   */
  public static GetStatusPOptions getGetStatusOptions() {
    return GetStatusPOptions.newBuilder()
        .setCommonOptions(getCommonOptions())
        .setLoadMetadataType(LoadMetadataPType.ONCE)
        .build();
  }

  /**
   * @return Master side defaults for {@link ListStatusPOptions}
   */
  public static ListStatusPOptions getListStatusOptions() {
    return ListStatusPOptions.newBuilder()
            .setCommonOptions(getCommonOptions())
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .build();
  }

  /**
   * @return Master side defaults for {@link LoadMetadataPOptions}
   */
  public static LoadMetadataPOptions getLoadMetadataOptions() {
    return LoadMetadataPOptions.newBuilder()
            .setCommonOptions(getCommonOptions())
            .setRecursive(false)
            .setCreateAncestors(false)
            .setLoadDescendantType(LoadDescendantPType.NONE)
            .build();
  }

  /**
   * @return Master side defaults for {@link MountPOptions}
   */
  public static MountPOptions getMountOptions() {
    return MountPOptions.newBuilder()
            .setCommonOptions(getCommonOptions())
            .setShared(false)
            .setReadOnly(false)
            .build();
  }

  /**
   * @return Master side defaults for {@link RenamePOptions}
   */
  public static RenamePOptions getRenameOptions() {
    return RenamePOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  /**
   * @return Master side defaults for {@link SetAttributePOptions}
   */
  public static SetAttributePOptions getSetAttributeOptions() {
    return SetAttributePOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setTtlAction(TtlAction.DELETE).setRecursive(false).setPinned(false).build();
  }

  /**
   * @return Master side defaults for {@link SetAclPOptions}
   */
  public static SetAclPOptions getSetAclOptions() {
    return SetAclPOptions.newBuilder().setCommonOptions(getCommonOptions()).setRecursive(false)
        .build();
  }

  /**
   * @return Master side defaults for {@link CheckConsistencyPOptions}
   */
  public static CheckConsistencyPOptions getCheckConsistencyOptions() {
    return CheckConsistencyPOptions.newBuilder().setCommonOptions(getCommonOptions()).build();
  }

  /**
   * @return Master side defaults for {@link SyncMetadataOptions}
   */
  public static SyncMetadataOptions getSyncMetadataOptions() {
    return SyncMetadataOptions.defaults();
  }
}
