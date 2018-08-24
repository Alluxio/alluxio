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

import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CommonOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
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
  public DeleteOptions getDeleteOptions() {
    return DeleteOptions.defaults();
  }

  @Override
  public FreeOptions getFreeOptions() {
    return FreeOptions.defaults();
  }

  @Override
  public GetStatusOptions getGetStatusOptions() {
    return GetStatusOptions.defaults();
  }

  @Override
  public LoadMetadataOptions getLoadMetadataOptions() {
    return LoadMetadataOptions.defaults();
  }

  @Override
  public MountOptions getMountOptions() {
    return MountOptions.defaults();
  }

   @Override
  public RenameOptions getRenameOptions() {
    return RenameOptions.defaults();
  }

  @Override
  public SetAttributeOptions getSetAttributeOptions() {
    return SetAttributeOptions.defaults();
  }

  @Override
  public SyncMetadataOptions getSyncMetadataOptions() {
    return SyncMetadataOptions.defaults();
  }
}
