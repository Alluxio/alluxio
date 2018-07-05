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

package alluxio.metrics;

/**
 * Metrics of Alluxio master.
 */
public final class MasterMetrics {
  // metrics names for FileSystemMaster
  public static final String DIRECTORIES_CREATED = "DirectoriesCreated";
  public static final String FILE_BLOCK_INFOS_GOT = "FileBlockInfosGot";
  public static final String FILE_INFOS_GOT = "FileInfosGot";
  public static final String FILES_COMPLETED = "FilesCompleted";
  public static final String FILES_CREATED = "FilesCreated";
  public static final String FILES_FREED = "FilesFreed";
  public static final String FILES_PERSISTED = "FilesPersisted";
  public static final String NEW_BLOCKS_GOT = "NewBlocksGot";
  public static final String PATHS_DELETED = "PathsDeleted";
  public static final String PATHS_MOUNTED = "PathsMounted";
  public static final String PATHS_RENAMED = "PathsRenamed";
  public static final String PATHS_UNMOUNTED = "PathsUnmounted";
  public static final String COMPLETE_FILE_OPS = "CompleteFileOps";
  public static final String CREATE_DIRECTORIES_OPS = "CreateDirectoryOps";
  public static final String CREATE_FILES_OPS = "CreateFileOps";
  public static final String DELETE_PATHS_OPS = "DeletePathOps";
  public static final String FREE_FILE_OPS = "FreeFileOps";
  public static final String GET_FILE_BLOCK_INFO_OPS = "GetFileBlockInfoOps";
  public static final String GET_FILE_INFO_OPS = "GetFileInfoOps";
  public static final String GET_NEW_BLOCK_OPS = "GetNewBlockOps";
  public static final String MOUNT_OPS = "MountOps";
  public static final String RENAME_PATH_OPS = "RenamePathOps";
  public static final String SET_ATTRIBUTE_OPS = "SetAttributeOps";
  public static final String UNMOUNT_OPS = "UnmountOps";
  public static final String FILES_PINNED = "FilesPinned";
  public static final String PATHS_TOTAL = "PathsTotal";
  public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
  public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
  public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";

  private MasterMetrics() {} // prevent instantiation
}
