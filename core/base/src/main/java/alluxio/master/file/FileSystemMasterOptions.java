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

import alluxio.file.options.CompleteFileOptions;
import alluxio.file.options.CreateDirectoryOptions;
import alluxio.file.options.CreateFileOptions;
import alluxio.file.options.CommonOptions;
import alluxio.file.options.SyncMetadataOptions;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;

/**
 * The interface for file system master default options.
 */
public interface FileSystemMasterOptions {
  /**
   * @return an instance of {@link CheckConsistencyPOptions}
   */
  CheckConsistencyPOptions getCheckConsistencyOptions();

  /**
   * @return an instance of {@link CommonOptions}
   */
  CommonOptions getCommonOptions();

  /**
   * TODO(ggezer) Merge with above after integrations are complete.
   * @return an instance of {@link alluxio.grpc.FileSystemMasterCommonPOptions}
   */
  FileSystemMasterCommonPOptions getCommonPOptions();

  /**
   * @return an instance of {@link CompleteFileOptions}
   */
  CompleteFileOptions getCompleteFileOptions();

  /**
   * @return an instance of {@link CreateFileOptions}
   */
  CreateFileOptions getCreateFileOptions();

  /**
   * @return an instance of {@link CreateDirectoryOptions}
   */
  CreateDirectoryOptions getCreateDirectoryOptions();

  /**
   * @return an instance of {@link DeletePOptions}
   */
  DeletePOptions getDeleteOptions();

  /**
   * @return an instance of {@link FreePOptions}
   */
  FreePOptions getFreeOptions();

  /**
   * @return an instance of {@link GetStatusPOptions}
   */
  GetStatusPOptions getGetStatusOptions();

  /**
   * @return an instance of {@link ListStatusPOptions}
   */
  ListStatusPOptions getListStatusOptions();

  /**
   * @return an instance of {@link LoadMetadataPOptions}
   */
  LoadMetadataPOptions getLoadMetadataOptions();

  /**
   * @return an instance of {@link MountPOptions}
   */
  MountPOptions getMountOptions();

  /**
   * @return an instance of {@link RenamePOptions}
   */
  RenamePOptions getRenameOptions();

  /**
   * @return an instance of {@link SetAclPOptions}
   */
  SetAclPOptions getSetAclOptions();

  /**
   * @return an instance of {@link SetAttributePOptions}
   */
  SetAttributePOptions getSetAttributeOptions();

  /**
   * @return an instance of {@link SyncMetadataOptions}
   */
  SyncMetadataOptions getSyncMetadataOptions();
}
