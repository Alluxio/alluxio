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
import alluxio.file.options.ListStatusOptions;
import alluxio.file.options.LoadMetadataOptions;
import alluxio.file.options.MountOptions;
import alluxio.file.options.RenameOptions;
import alluxio.file.options.SetAclOptions;
import alluxio.file.options.SetAttributeOptions;
import alluxio.file.options.SyncMetadataOptions;

/**
 * The interface for file system master default options.
 */
public interface FileSystemMasterOptions {
  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link CheckConsistencyOptions}
   */
  <T extends CheckConsistencyOptions> T getCheckConsistencyOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link CommonOptions}
   */
  <T extends CommonOptions> T getCommonOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link CompleteFileOptions}
   */
  <T extends CompleteFileOptions> T getCompleteFileOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link CreateFileOptions}
   */
  <T extends CreateFileOptions> T getCreateFileOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link CreateDirectoryOptions}
   */
  <T extends CreateDirectoryOptions> T getCreateDirectoryOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link DeleteOptions}
   */
  <T extends DeleteOptions> T getDeleteOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link FreeOptions}
   */
  <T extends FreeOptions> T getFreeOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link GetStatusOptions}
   */
  <T extends GetStatusOptions> T getGetStatusOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link ListStatusOptions}
   */
  <T extends ListStatusOptions> T getListStatusOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link LoadMetadataOptions}
   */
  <T extends LoadMetadataOptions> T getLoadMetadataOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link MountOptions}
   */
  <T extends MountOptions> T getMountOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link RenameOptions}
   */
  <T extends RenameOptions> T getRenameOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link SetAclOptions}
   */
  <T extends SetAclOptions> T getSetAclOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link SetAttributeOptions}
   */
  <T extends SetAttributeOptions> T getSetAttributeOptions();

  /**
   * @param <T> the type of the concrete options subclass
   * @return an instance of {@link SyncMetadataOptions}
   */
  <T extends SyncMetadataOptions> T getSyncMetadataOptions();
}
