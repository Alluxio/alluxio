namespace java tachyon.thrift

// Version 4: 0.8.0
// Version 3: 0.6.0
// Version 2: 0.5.0
// Version 1: before 0.5.0

// Information about workers.
struct NetAddress {
  1: string host
  2: i32 rpcPort
  3: i32 dataPort
}

struct WorkerInfo {
  1: i64 id
  2: NetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 startTimeMs
}

// Information about blocks.
struct BlockLocation {
  1: i64 workerId
  2: NetAddress workerAddress
  3: i32 tier
}

// Contains the information of a block in Tachyon. It maintains the worker nodes where the replicas
// of the blocks are stored.
struct BlockInfo {
  1: i64 blockId
  2: i64 length
  3: list<BlockLocation> locations
}

// Contains the information of a block in a file. In addition to the BlockInfo, it includes the
// offset in the file, and the under file system locations of the block replicas
struct FileBlockInfo {
  1: BlockInfo blockInfo
  2: i64 offset
  3: list<NetAddress> ufsLocations
}

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
  14: i32 dependencyId
  15: i32 inMemoryPercentage
  16: i64 lastModificationTimeMs
  17: i64 ttl
}

// Information about lineage.
struct DependencyInfo {
  1: i32 id
  2: list<i64> parents
  3: list<i64> children
  4: list<binary> data
}

// Information about raw tables.
struct RawTableInfo {
  1: i64 id
  2: string name
  3: string path
  4: i32 columns
  5: binary metadata
}

enum CommandType {
  Unknown = 0,
  Nothing = 1,
  Register = 2,   	// Ask the worker to re-register.
  Free = 3,		// Ask the worker to free files.
  Delete = 4,		// Ask the worker to delete files.
}

struct Command {
  1: CommandType mCommandType
  2: list<i64> mData
}

exception BlockInfoException {
  1: string message
}

exception OutOfSpaceException {
  1: string message
}

exception FailedToCheckpointException {
  1: string message
}

exception FileAlreadyExistException {
  1: string message
}

exception FileDoesNotExistException {
  1: string message
}

exception NoWorkerException {
  1: string message
}

exception SuspectedFileSizeException {
  1: string message
}

exception InvalidPathException {
  1: string message
}

exception TableColumnException {
  1: string message
}

exception TableDoesNotExistException {
  1: string message
}

exception TachyonException {
  1: string message
}

exception DependencyDoesNotExistException {
  1: string message
}

service BlockMasterService {
  i64 workerGetWorkerId(1: NetAddress workerNetAddress)

  void workerRegister(1: i64 workerId, 2: list<i64> totalBytesOnTiers,
      3: list<i64> usedBytesOnTiers, 4: map<i64, list<i64>> currentBlocksOnTiers)
    throws (1: TachyonException te)

  Command workerHeartbeat(1: i64 workerId, 2: list<i64> usedBytesOnTiers,
      3: list<i64> removedBlockIds, 4: map<i64, list<i64>> addedBlocksOnTiers)
    throws (1: BlockInfoException bie)

  void workerCommitBlock(1: i64 workerId, 2: i64 usedBytesOnTier, 3: i32 tier, 4: i64 blockId,
      5: i64 length)
    throws (1: BlockInfoException bie)

  list<WorkerInfo> getWorkerInfoList()

  i64 getCapacityBytes()

  i64 getUsedBytes()

  BlockInfo getBlockInfo(1: i64 blockId) throws (1: BlockInfoException bie)
}

service FileSystemMasterService {
  set<i64> workerGetPinIdList()

  list<i32> workerGetPriorityDependencyList()

  i64 getFileId(1: string path)
    throws (1: InvalidPathException ipe)

  FileInfo getFileInfo(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee)

  list<FileInfo> getFileInfoList(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee)

  FileBlockInfo getFileBlockInfo(1: i64 fileId, 2: i32 fileBlockIndex)
    throws (1: FileDoesNotExistException fdnee, 2: BlockInfoException bie)

  list<FileBlockInfo> getFileBlockInfoList(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee)

  i64 getNewBlockIdForFile(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee, 2: BlockInfoException bie)

  // TODO(gene): Is this necessary?
  string getUfsAddress()

  i64 createFile(1: string path, 2: i64 blockSizeBytes, 3: bool recursive, 4: i64 ttl)
    throws (1: FileAlreadyExistException faee, 2: BlockInfoException bie,
      3: SuspectedFileSizeException sfse, 4: TachyonException te)

  void completeFile(1: i64 fileId)
    throws (1: BlockInfoException bie, 2: FileDoesNotExistException fdnee,
      3: InvalidPathException ipe)

  bool deleteFile(1: i64 fileId, 2: bool recursive)
    throws (1: TachyonException te)

  bool renameFile(1: i64 fileId, 2: string dstPath)
    throws (1:FileAlreadyExistException faee, 2: FileDoesNotExistException fdnee,
      3: InvalidPathException ipe)

  void setPinned(1: i64 fileId, 2: bool pinned)
    throws (1: FileDoesNotExistException fdnee)

  bool createDirectory(1: string path, 2: bool recursive)
    throws (1: FileAlreadyExistException faee, 2: InvalidPathException ipe)

  bool free(1: i64 fileId, 2: bool recursive)
    throws (1: FileDoesNotExistException fdnee)

  bool persistFile(1: i64 fileId, 2: i64 length)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: BlockInfoException eB)

  /**
   * Loads metadata for the file identified by the given Tachyon path from UFS into Tachyon.
   */
  i64 loadFileInfoFromUfs(1: string ufsPath, 2: bool recursive)
    throws (1: BlockInfoException bie, 2: FileDoesNotExistException fdnee,
    3: FileAlreadyExistException faee, 4: InvalidPathException ipe,
    5: SuspectedFileSizeException sfse, 6: TachyonException te)

  /**
   * Creates a new "mount point", mounts the given UFS path in the Tachyon namespace at the given
   * path. The path should not exist and should not be nested under any existing mount point.
   */
  bool mount(1: string tachyonPath, 2: string ufsPath)
    throws (1: TachyonException te)

  /**
   * Deletes an existing "mount point", voiding the Tachyon namespace at the given path. The path
   * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
   * will be persisted before they are removed from the Tachyon namespace.
   */
  bool unmount(1: string tachyonPath) throws (1: TachyonException te)

  // Lineage Features
  i32 createDependency(1: list<string> parents, 2: list<string> children,
      3: string commandPrefix, 4: list<binary> data, 5: string comment, 6: string framework,
      7: string frameworkVersion, 8: i32 dependencyType, 9: i64 childrenBlockSizeByte)
    throws (1: InvalidPathException ipe, 2: FileDoesNotExistException fdnee,
      3: FileAlreadyExistException faee, 4: BlockInfoException bie, 5: TachyonException te)

  DependencyInfo getDependencyInfo(1: i32 dependencyId)
    throws (1: DependencyDoesNotExistException ddnee)

  void reportLostFile(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee)

  void requestFilesInDependency(1: i32 depId)
    throws (1: DependencyDoesNotExistException ddnee)
}

service RawTableMasterService {
  i64 createRawTable(1: string path, 2: i32 columns, 3: binary metadata)
    throws (1: FileAlreadyExistException faee, 2: InvalidPathException ipe, 3: TableColumnException tce,
      4: TachyonException te)

  i64 getRawTableId(1: string path)
    throws (1: InvalidPathException ipe, 2: TableDoesNotExistException tdnee)

  RawTableInfo getClientRawTableInfoById(1: i64 id)
    throws (1: TableDoesNotExistException tdnee)

  RawTableInfo getClientRawTableInfoByPath(1: string path)
    throws (1: TableDoesNotExistException tdnee, 2: InvalidPathException ipe)

  void updateRawTableMetadata(1: i64 tableId, 2: binary metadata)
    throws (1: TableDoesNotExistException tdnee, 2: TachyonException te)
}

service WorkerService {
  void accessBlock(1: i64 blockId)

  void persistFile(1: i64 fileId, 2: i64 nonce, 3: string path)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: FailedToCheckpointException eF, 4: BlockInfoException eB)

  bool asyncCheckpoint(1: i64 fileId)
    throws (1: TachyonException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: FileDoesNotExistException eP, 2: BlockInfoException eB)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock(1: i64 sessionId, 2: i64 blockId)

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   */
  string lockBlock(1: i64 blockId, 2: i64 sessionId)
    throws (1: FileDoesNotExistException eP)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   */
  bool promoteBlock(1: i64 blockId)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistException will be thrown.
   */
  string requestBlockLocation(1: i64 sessionId, 2: i64 blockId, 3: i64 initialBytes)
    throws (1: OutOfSpaceException eP, 2: FileAlreadyExistException eS)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace(1: i64 sessionId, 2: i64 blockId, 3: i64 requestBytes)
    throws (1: FileDoesNotExistException eP)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock(1: i64 blockId, 2: i64 sessionId)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   */
  void sessionHeartbeat(1: i64 sessionId, 2: list<i64> metrics)
}
