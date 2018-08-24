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

import alluxio.file.options.CheckConsistencyOptions;
import alluxio.file.options.CompleteFileOptions;
import alluxio.file.options.CreateDirectoryOptions;
import alluxio.file.options.CreateFileOptions;
import alluxio.file.options.DeleteOptions;
import alluxio.file.options.FreeOptions;
import alluxio.file.options.GetStatusOptions;
import alluxio.file.options.CommonOptions;
import alluxio.file.options.LoadMetadataOptions;
import alluxio.file.options.MountOptions;
import alluxio.file.options.RenameOptions;
import alluxio.file.options.SetAttributeOptions;
import alluxio.file.options.SyncMetadataOptions;

/**
 * The interface for file system master default options.
 */
public interface FileSystemMasterOptions {
  CheckConsistencyOptions getCheckConsistencyOptions();

  CommonOptions getCommonOptions();

  CompleteFileOptions getCompleteFileOptions();

  CreateDirectoryOptions getCreateDirectoryOptions();

  CreateFileOptions getCreateFileOptions();

  DeleteOptions getDeleteOptions();

  FreeOptions getFreeOptions();

  GetStatusOptions getGetStatusOptions();

  LoadMetadataOptions getLoadMetadataOptions();

  MountOptions getMountOptions();

  RenameOptions getRenameOptions();

  SetAttributeOptions getSetAttributeOptions();

  SyncMetadataOptions getSyncMetadataOptions();
}
