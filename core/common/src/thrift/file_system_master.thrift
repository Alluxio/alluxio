namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct CheckConsistencyTOptions {}
struct CheckConsistencyTResponse {
  1: list<string> inconsistentPaths
}

struct CompleteFileTOptions {
  1: optional i64 ufsLength
}
struct CompleteFileTResponse {}

struct CreateDirectoryTOptions {
  1: optional bool persisted
  2: optional bool recursive
  3: optional bool allowExists
  4: optional i16 mode
  5: optional i64 ttl
  6: optional common.TTtlAction ttlAction
}
struct CreateDirectoryTResponse {}

struct CreateFileTOptions {
  1: optional i64 blockSizeBytes
  2: optional bool persisted
  3: optional bool recursive
  4: optional i64 ttl
  5: optional i16 mode
  6: optional common.TTtlAction ttlAction
}
struct CreateFileTResponse {}

struct DeleteTOptions {
  1: optional bool recursive
  2: optional bool alluxioOnly
  3: optional bool unchecked
}
struct DeleteTResponse {}

struct FreeTOptions {
  1: optional bool recursive
  2: optional bool forced
}
struct FreeTResponse {}

enum LoadMetadataTType {
  Never = 0,  // Never load metadata.
  Once = 1,  // Load metadata only once.
  Always = 2,  // Always load metadata.
}

struct GetStatusTOptions {
  1: optional LoadMetadataTType loadMetadataType
}
struct GetStatusTResponse {
  1: FileInfo fileInfo
}

struct GetNewBlockIdForFileTOptions {}
struct GetNewBlockIdForFileTResponse {
  1: i64 id
}

struct ListStatusTOptions {
  // This is deprecated since 1.1.1 and will be removed in 2.0. Use loadMetadataType.
  1: optional bool loadDirectChildren
  2: optional LoadMetadataTType loadMetadataType
}
struct ListStatusTResponse {
  1: list<FileInfo> fileInfoList
}

struct LoadMetadataTOptions {}
struct LoadMetadataTResponse {
  1: i64 id
}

/**
* Contains the information of a block in a file. In addition to the BlockInfo, it includes the
* offset in the file, and the under file system locations of the block replicas.
*/
struct FileBlockInfo {
  1: common.BlockInfo blockInfo
  2: i64 offset
  3: list<common.WorkerNetAddress> ufsLocations // deprecated since 1.1 will be removed in 2.0 (replaced by ufsStringLocations)
  4: list<string> ufsStringLocations
}

struct FileInfo {
  1: i64 fileId
  2: string name
  3: string path
  4: string ufsPath
  5: i64 length
  6: i64 blockSizeBytes
  7: i64 creationTimeMs
  8: bool completed
  9: bool folder
  10: bool pinned
  11: bool cacheable
  12: bool persisted
  13: list<i64> blockIds
  15: i32 inMemoryPercentage
  16: i64 lastModificationTimeMs
  17: i64 ttl
  18: string owner
  19: string group
  20: i32 mode
  21: string persistenceState
  22: bool mountPoint
  23: list<FileBlockInfo> fileBlockInfos
  24: common.TTtlAction ttlAction
  25: i64 mountId
}

struct MountTOptions {
  1: optional bool readOnly
  2: optional map<string, string> properties
  3: optional bool shared
}
struct MountTResponse {}

struct GetMountTableTResponse {
  1: map<string, MountPointInfo> mountTable
}

struct MountPointInfo {
  1: string ufsUri
  2: string ufsType
  3: i64 ufsCapacityBytes = -1
  4: i64 ufsUsedBytes = -1
  5: bool readOnly
  6: map<string, string> properties
  7: bool shared
}

struct FileSystemCommand {
  1: common.CommandType commandType
  2: FileSystemCommandOptions commandOptions
}

union FileSystemCommandOptions {
  1: optional PersistCommandOptions persistOptions
}

struct PersistCommandOptions {
  1: list<PersistFile> persistFiles
}

struct PersistFile {
  1: i64 fileId
  2: list<i64> blockIds
}

struct RenameTOptions {}
struct RenameTResponse {}

struct SetAttributeTOptions {
  1: optional bool pinned
  2: optional i64 ttl
  3: optional bool persisted
  4: optional string owner
  5: optional string group
  6: optional i16 mode
  7: optional bool recursive
  8: optional common.TTtlAction ttlAction
}
struct SetAttributeTResponse {}

struct ScheduleAsyncPersistenceTOptions {}
struct ScheduleAsyncPersistenceTResponse {}

struct UnmountTOptions {}
struct UnmountTResponse {}

struct UfsInfo {
  1: optional string uri
  2: optional MountTOptions properties
}

/**
 * This interface contains file system master service endpoints for Alluxio clients.
 */
service FileSystemMasterClientService extends common.AlluxioService {

  /**
   * Checks the consistency of the files and directores with the path as the root of the subtree
   */
  CheckConsistencyTResponse checkConsistency(
    /** the root of the subtree to check */ 1: string path,
    /** the method options */ 2: CheckConsistencyTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Marks a file as completed.
   */
  CompleteFileTResponse completeFile(
    /** the path of the file */ 1: string path,
    /** the method options */ 2: CompleteFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a directory.
   */
  CreateDirectoryTResponse createDirectory(
    /** the path of the directory */ 1: string path,
    /** the method options */ 2: CreateDirectoryTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a file.
   */
  CreateFileTResponse createFile(
    /** the path of the file */ 1: string path,
    /** the options for creating the file */ 2: CreateFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Frees the given file or directory from Alluxio.
   */
  FreeTResponse free(
    /** the path of the file or directory */ 1: string path,
    // This is deprecated since 1.5 and will be removed in 2.0. Use FreeTOptions.
    /** whether to free recursively */ 2: bool recursive,
    /** the options for freeing a path */ 3: FreeTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the status of the file or directory.
   */
  GetStatusTResponse getStatus(
    /** the path of the file or directory */ 1: string path,
    /** the method options */ 2: GetStatusTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Generates a new block id for the given file.
   */
  GetNewBlockIdForFileTResponse getNewBlockIdForFile(
    /** the path of the file */ 1: string path,
    /** the method options */ 2: GetNewBlockIdForFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * If the path points to a file, the method returns a singleton with its file information.
   * If the path points to a directory, the method returns a list with file information for the
   * directory contents.
   */
  ListStatusTResponse listStatus(
    /** the path of the file or directory */ 1: string path,
    /** listStatus options */ 2: ListStatusTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Loads metadata for the object identified by the given Alluxio path from UFS into Alluxio.
   *
   * THIS METHOD IS DEPRECATED SINCE VERSION 1.1 AND WILL BE REMOVED IN VERSION 2.0.
   */
  LoadMetadataTResponse loadMetadata(
    /** the path of the under file system */ 1: string ufsPath,
    /** whether to load metadata recursively */ 2: bool recursive,
    /** the method options */ 3: LoadMetadataTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
   * path. The path should not exist and should not be nested under any existing mount point.
   */
  MountTResponse mount(
    /** the path of alluxio mount point */ 1: string alluxioPath,
    /** the path of the under file system */ 2: string ufsPath,
    /** the options for creating the mount point */ 3: MountTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
  * Returns a map from each Alluxio path to information of corresponding mount point
  */
  GetMountTableTResponse getMountTable()
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes a file or a directory and returns whether the remove operation succeeded.
   * NOTE: Unfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
   */
  DeleteTResponse remove(
    /** the path of the file or directory */ 1: string path,
    // This is deprecated since 1.5 and will be removed in 2.0. Use DeleteTOptions.
    /** whether to remove recursively */ 2: bool recursive,
    /** the options for deleting the file */ 3: DeleteTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Renames a file or a directory.
   */
  RenameTResponse rename(
    /** the source path of the file or directory */ 1: string path,
    /** the desination path of the file */ 2: string dstPath,
    /** the method options */ 3: RenameTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Sets file or directory attributes.
   */
  SetAttributeTResponse setAttribute(
    /** the path of the file or directory */ 1: string path,
    /** the method options */ 2: SetAttributeTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Schedules async persistence.
   */
  ScheduleAsyncPersistenceTResponse scheduleAsyncPersistence(
    /** the path of the file */ 1: string path,
    /** the method options */ 2: ScheduleAsyncPersistenceTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
   * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
   * will be persisted before they are removed from the Alluxio namespace.
   */
  UnmountTResponse unmount(
    /** the path of the alluxio mount point */ 1: string alluxioPath,
    /** the method options */ 2: UnmountTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}

struct FileSystemHeartbeatTOptions {}
struct FileSystemHeartbeatTResponse {
  1: FileSystemCommand command
}

struct GetFileInfoTOptions {}
struct GetFileInfoTResponse {
  1: FileInfo fileInfo
}

struct GetPinnedFileIdsTOptions {}
struct GetPinnedFileIdsTResponse {
  1: set<i64> pinnedFileIds
}

struct GetUfsInfoTOptions {}
struct GetUfsInfoTResponse {
  1: UfsInfo ufsInfo
}

/**
 * This interface contains file system master service endpoints for Alluxio workers.
 */
service FileSystemMasterWorkerService extends common.AlluxioService {

  /**
   * Periodic file system worker heartbeat. Returns the command for persisting
   * the blocks of a file.
   */
  FileSystemHeartbeatTResponse fileSystemHeartbeat(
    /** the id of the worker */ 1: i64 workerId,
    /** the list of persisted files */ 2: list<i64> persistedFiles,
    /** the method options */ 3: FileSystemHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /*
   * Returns the file information for a file or directory identified by the given file id.
   */
  GetFileInfoTResponse getFileInfo(
    /** the id of the file */ 1: i64 fileId,
    /** the method options */ 2: GetFileInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the set of pinned file ids.
   */
  GetPinnedFileIdsTResponse getPinnedFileIds(
    /** the method options */ 1: GetPinnedFileIdsTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns the UFS information for the given mount point identified by its id.
   **/
  GetUfsInfoTResponse getUfsInfo(
    /** the id of the ufs */ 1: i64 mountId,
    /** the method options */ 2: GetUfsInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
