namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

struct FileInfo {
  1: i64 fileId
  2: string name
  3: string path
  4: string ufsPath
  5: i64 length
  6: i64 blockSizeBytes
  7: i64 creationTimeMs
  8: bool isCompleted
  9: bool isFolder
  10: bool isPinned
  11: bool isCacheable
  12: bool isPersisted
  13: list<i64> blockIds
  15: i32 inMemoryPercentage
  16: i64 lastModificationTimeMs
  17: i64 ttl
}

struct CreateTOptions {
  1: optional i64 blockSizeBytes
  2: optional bool persisted
  3: optional bool recursive
  4: optional i64 ttl
}

struct MkdirTOptions {
  1: optional bool persisted
  2: optional bool recursive
}

service FileSystemMasterService {

  // Tachyon Client API

  /*
   * Marks a file as completed.
   */
  void completeFile(1: i64 fileId) throws (1: exception.TachyonTException e)

  /*
   * Creates a file.
   */
  i64 create(1: string path, 2: CreateTOptions options)
    throws (1: exception.TachyonTException e)

  /*
   * Frees the given file from Tachyon.
   */
  bool free(1: i64 fileId, 2: bool recursive)
    throws (1: exception.TachyonTException e)

  /*
   * Returns the file block information for the given file and file block index.
   */
  common.FileBlockInfo getFileBlockInfo(1: i64 fileId, 2: i32 fileBlockIndex)
    throws (1: exception.TachyonTException e)

  /*
   * Returns the list of file blocks information for the given file.
   */
  list<common.FileBlockInfo> getFileBlockInfoList(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * Returns the file id for the given path.
   */
  i64 getFileId(1: string path)

  /*
   * Returns the file information.
   */
  FileInfo getFileInfo(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * If the id points to a file, the method returns a singleton with its file information.
   * If the id points to a directory, the method returns a list with file information for the
   * directory contents.
   */
  list<FileInfo> getFileInfoList(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * Generates a new block id for the given file.
   */
  i64 getNewBlockIdForFile(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * Returns the UFS address of the root mount point.
   */
  // TODO(gene): Is this necessary?
  string getUfsAddress()

  /*
   * Loads metadata for the object identified by the given Tachyon path from UFS into Tachyon.
   */
  // TODO(jiri): Get rid of this.
  i64 loadMetadata(1: string ufsPath, 2: bool recursive)
    throws (1: exception.TachyonTException e)

  bool mkdir(1: string path, 2: MkdirTOptions options)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /*
   * Creates a new "mount point", mounts the given UFS path in the Tachyon namespace at the given
   * path. The path should not exist and should not be nested under any existing mount point.
   */
  bool mount(1: string tachyonPath, 2: string ufsPath)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  bool persistFile(1: i64 fileId, 2: i64 length)
    throws (1: exception.TachyonTException e)

  /*
   * Deletes a file or a directory.
   *
   * NOTE: Unfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
   */
  bool remove(1: i64 id, 2: bool recursive)
    throws (1: exception.TachyonTException e)

  /*
   * Renames a file or a directory.
   */
  bool rename(1: i64 fileId, 2: string dstPath)
    throws (1: exception.TachyonTException e)

  /*
   * Reports file as lost.
   */
  void reportLostFile(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * Sets the pinned flag for a file.
   */
  void setPinned(1: i64 fileId, 2: bool pinned)
    throws (1: exception.TachyonTException e)


  void setTTL(1: i64 fileId, 2: i64 ttl)
    throws (1: exception.TachyonTException e)

  /*
   * Deletes an existing "mount point", voiding the Tachyon namespace at the given path. The path
   * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
   * will be persisted before they are removed from the Tachyon namespace.
   */
  bool unmount(1: string tachyonPath)
  throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  // Tachyon Worker API

  /*
   * Retursn the set of pinned files.
   */
  set<i64> workerGetPinIdList()
}
