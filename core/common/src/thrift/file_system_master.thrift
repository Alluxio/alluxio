namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct CompleteFileTOptions {
  1: optional i64 ufsLength
}

struct CreateDirectoryTOptions {
  1: optional bool persisted
  2: optional bool recursive
  3: optional bool allowExists
}

struct CreateFileTOptions {
  1: optional i64 blockSizeBytes
  2: optional bool persisted
  3: optional bool recursive
  4: optional i64 ttl
}

struct MountTOptions {
  1: optional bool readOnly
  2: optional map<string, string> properties
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
  18: string userName
  19: string groupName
  20: i32 permission
  21: string persistenceState
  22: bool mountPoint
}

struct FileSystemCommand {
  1: common.CommandType commandType
  2: FileSystemCommandOptions commandOptions
}

struct PersistCommandOptions {
  1: list<PersistFile> persistFiles
}

struct PersistFile {
  1: i64 fileId
  2: list<i64> blockIds
}

struct SetAttributeTOptions {
  1: optional bool pinned
  2: optional i64 ttl
  3: optional bool persisted
  4: optional string owner
  5: optional string group
  6: optional i16 permission
  7: optional bool recursive
}

union FileSystemCommandOptions {
  1: optional PersistCommandOptions persistOptions
}

/**
 * This interface contains file system master service endpoints for Alluxio clients.
 */
service FileSystemMasterClientService extends common.AlluxioService {

  /**
   * Marks a file as completed.
   */
  void completeFile( /** the path of the file */ 1: string path,
      /** the method options */ 2: CompleteFileTOptions options)
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a directory.
   */
  void createDirectory( /** the path of the directory */ 1: string path,
      /** the method options */ 2: CreateDirectoryTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Creates a file.
   */
  void createFile( /** the path of the file */ 1: string path,
      /** the options for creating the file */ 2: CreateFileTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Frees the given file or directory from Alluxio.
   */
  void free( /** the path of the file or directory */ 1: string path,
      /** whether to free recursively */ 2: bool recursive)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the list of file blocks information for the given file.
   */
  list<common.FileBlockInfo> getFileBlockInfoList( /** the path of the file */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the status of the file or directory.
   */
  FileInfo getStatus( /** the path of the file or directory */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the status of the file or directory, only used internally by servers.
   */
  FileInfo getStatusInternal( /** the id of the file or directory */ 1: i64 fileId)
    throws (1: exception.AlluxioTException e)

  /**
   * Generates a new block id for the given file.
   */
  i64 getNewBlockIdForFile( /** the path of the file */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the UFS address of the root mount point.
   *
   * THIS METHOD IS DEPRECATED SINCE VERSION 1.1 AND WILL BE REMOVED IN VERSION 2.0.
   */
  string getUfsAddress()

  /**
   * If the path points to a file, the method returns a singleton with its file information.
   * If the path points to a directory, the method returns a list with file information for the
   * directory contents.
   */
  list<FileInfo> listStatus( /** the path of the file or directory */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Loads metadata for the object identified by the given Alluxio path from UFS into Alluxio.
   */
  // TODO(jiri): Get rid of this.
  i64 loadMetadata( /** the path of the under file system */ 1: string ufsPath,
      /** whether to load meta data recursively */ 2: bool recursive)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
   * path. The path should not exist and should not be nested under any existing mount point.
   */
  void mount( /** the path of alluxio mount point */ 1: string alluxioPath,
      /** the path of the under file system */ 2: string ufsPath,
      /** the options for creating the mount point */ 3: MountTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Deletes a file or a directory and returns whether the remove operation succeeded.
   * NOTE: Unfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
   */
  void remove( /** the path of the file or directory */ 1: string path,
      /** whether to remove recursively */ 2: bool recursive)
    throws (1: exception.AlluxioTException e)

  /**
   * Renames a file or a directory.
   */
  void rename( /** the path of the file or directory */ 1: string path,
      /** the desinationpath of the file */ 2: string dstPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Sets file or directory attributes.
   */
  void setAttribute( /** the path of the file or directory */ 1: string path,
       /** the method options */ 2: SetAttributeTOptions options)
    throws (1: exception.AlluxioTException e)

  /**
   * Schedules async persistence.
   */
  void scheduleAsyncPersist( /** the path of the file */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
   * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
   * will be persisted before they are removed from the Alluxio namespace.
   */
  void unmount( /** the path of the alluxio mount point */ 1: string alluxioPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}

/**
 * This interface contains file system master service endpoints for Alluxio workers.
 */
service FileSystemMasterWorkerService extends common.AlluxioService {

  /*
   * Returns the file information.
   */
  FileInfo getFileInfo( /** the id of the file */ 1: i64 fileId)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the set of pinned files.
   */
  set<i64> getPinIdList()

  /**
   * Periodic file system worker heartbeat. Returns the command for persisting
   * the blocks of a file.
   */
  FileSystemCommand heartbeat( /** the id of the worker */ 1: i64 workerId,
      /** the list of persisted files */ 2: list<i64> persistedFiles)
    throws (1: exception.AlluxioTException e)
}
