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

package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.CheckConsistencyPOptions;
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
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;

/**
 * This interface defines default options for file system operations that are
 * common to client and master.
 */
public interface FileSystemOptionsProvider {
  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  ScheduleAsyncPersistencePOptions scheduleAsyncPersistenceDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  CreateDirectoryPOptions createDirectoryDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  CheckConsistencyPOptions checkConsistencyDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  CreateFilePOptions createFileDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  DeletePOptions deleteDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  ExistsPOptions existsDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  FileSystemMasterCommonPOptions commonDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  FreePOptions freeDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  GetStatusPOptions getStatusDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  ListStatusPOptions listStatusDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  LoadMetadataPOptions loadMetadataDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  MountPOptions mountDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  OpenFilePOptions openFileDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  RenamePOptions renameDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  ScheduleAsyncPersistencePOptions scheduleAsyncPersistDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  SetAclPOptions setAclDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  SetAttributePOptions setAttributeDefaults(AlluxioConfiguration conf);

  /**
   * Defaults for the SetAttribute RPC which should only be used on the client side.
   *
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  SetAttributePOptions setAttributeClientDefaults(AlluxioConfiguration conf);

  /**
   * @param conf Alluxio configuration
   * @return options based on the configuration
   */
  UnmountPOptions unmountDefaults(AlluxioConfiguration conf);
}
