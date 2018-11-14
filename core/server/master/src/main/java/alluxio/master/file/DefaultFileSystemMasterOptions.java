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
import alluxio.grpc.*;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CommonOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.SyncMetadataOptions;

/**
 * The file system class to set default options.
 */
public final class DefaultFileSystemMasterOptions implements FileSystemMasterOptions {

  @Override
  public CheckConsistencyOptions getCheckConsistencyOptions() {
    return CheckConsistencyOptions.defaults();
  }

  @Override
  public CommonOptions getCommonOptions() {
    return CommonOptions.defaults();
  }

  public FileSystemMasterCommonPOptions getCommonPOptions() {
    return FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.NO_TTL)
        .setTtlAction(TtlAction.DELETE)
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .setOperationTimeMs(System.currentTimeMillis()).build();
  }

  @Override
  public CompleteFileOptions getCompleteFileOptions() {
    return CompleteFileOptions.defaults();
  }

  @Override
  public CreateDirectoryOptions getCreateDirectoryOptions() {
    return CreateDirectoryOptions.defaults();
  }

  @Override
  public CreateFileOptions getCreateFileOptions() {
    return CreateFileOptions.defaults();
  }

  @Override
  public DeletePOptions getDeleteOptions() {
    return DeletePOptions.newBuilder()
            .setRecursive(false)
            .setAlluxioOnly(false)
            .setUnchecked(false)
            .build();
  }

  @Override
  public FreePOptions getFreeOptions() {
    return FreePOptions.newBuilder()
        .setCommonOptions(getCommonPOptions())
        .setForced(false)
        .setRecursive(false)
        .build();
  }

  @Override
  public GetStatusPOptions getGetStatusOptions() {
    return GetStatusPOptions.newBuilder()
        .setCommonOptions(getCommonPOptions())
        .setLoadMetadataType(LoadMetadataPType.ONCE)
        .build();
  }

  @Override
  public ListStatusPOptions getListStatusOptions() {
    return ListStatusPOptions.newBuilder()
            .setCommonOptions(getCommonPOptions())
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .build();
  }

  @Override
  public LoadMetadataPOptions getLoadMetadataOptions() {
    return LoadMetadataPOptions.newBuilder()
            .setCommonOptions(getCommonPOptions())
            .setRecursive(false)
            .setCreateAncestors(false)
            .setLoadDescendantType(LoadDescendantPType.NONE)
            .build();
  }

  @Override
  public MountPOptions getMountOptions() {
    return MountPOptions.newBuilder()
            .setShared(false)
            .setReadOnly(false)
            .build();
  }

  @Override
  public RenamePOptions getRenameOptions() {
    return RenamePOptions.newBuilder().setCommonOptions(getCommonPOptions()).build();
  }

  @Override
  public SetAttributePOptions getSetAttributeOptions() {
    return SetAttributePOptions.newBuilder().setCommonOptions(getCommonPOptions())
        .setTtlAction(TtlAction.DELETE).setRecursive(false).build();
  }

  @Override
  public SetAclPOptions getSetAclOptions() {
    return SetAclPOptions.newBuilder().setCommonOptions(getCommonPOptions()).setRecursive(false)
        .build();
  }

  @Override
  public SyncMetadataOptions getSyncMetadataOptions() {
    return SyncMetadataOptions.defaults();
  }
}
