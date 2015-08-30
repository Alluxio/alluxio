namespace java tachyon.thrift

// Version 4: 0.8.0
// Version 3: 0.6.0
// Version 2: 0.5.0
// Version 1: before 0.5.0

// Information about workers.
struct NetAddress {
  1: string mHost
  2: i32 mPort
  3: i32 mSecondaryPort
}

struct WorkerInfo {
  1: i64 id
  2: NetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 starttimeMs
}

// Information about blocks.
struct BlockLocation {
  1: i64 workerId
  2: NetAddress workerAddress
  3: i32 tier
}

struct BlockInfo {
  1: i64 blockId
  2: i64 length
  3: list<BlockLocation> locations
}

// Information about files.
// TODO: Just include a BlockInfo in this FileBlockInfo
struct FileBlockInfo {
  1: i64 blockId
  2: i64 offset
  3: i64 length
  4: list<NetAddress> locations
}

// TODO: make fileId into i64
struct FileInfo {
  1: i32 fileId
  2: string name
  3: string path
  4: string ufsPath
  5: i64 length
  6: i64 blockSizeByte
  7: i64 creationTimeMs
  8: bool isComplete
  9: bool isFolder
  10: bool isPinned
  11: bool isCache
  12: list<i64> blockIds
  13: i32 dependencyId
  14: i32 inMemoryPercentage
  15: i64 lastModificationTimeMs
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
  1: i32 id
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

service UserMasterService {
  i64 getUserId()
}

service BlockMasterService {
  i64 workerGetWorkerId(1: NetAddress workerNetAddress)

  i64 workerRegister(1: i64 workerId, 2: list<i64> totalBytesOnTiers, 3: list<i64> usedBytesOnTiers,
      4: map<i64, list<i64>> currentBlocksOnTiers)
    throws (1: BlockInfoException bie)

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

  // TODO: is this necessary?
  string getUfsAddress()

  i64 createFile(1: string path, 2: i64 blockSizeBytes, 3: bool recursive)
    throws (1: FileAlreadyExistException faee, 2: BlockInfoException bie,
      3: SuspectedFileSizeException sfse, 4: TachyonException te)

  bool completeFileCheckpoint(1: i64 workerId, 2: i64 fileId, 3: i64 length,
      4: string checkpointPath)
    throws (1: FileDoesNotExistException fdnee, 2: SuspectedFileSizeException sfse,
      3: BlockInfoException bie)

  i64 loadFileFromUfs(1: i64 fileId, 2: string ufsPath, 3: bool recursive)
    throws (1: FileAlreadyExistException faee, 2: BlockInfoException bie,
      3: SuspectedFileSizeException sfse, 4: TachyonException te)

  void completeFile(1: i64 fileId)
    throws (1: FileDoesNotExistException fdnee, 2: BlockInfoException bie)

  bool deleteFile(1: i64 fileId, 2: bool recursive)
    throws (1: TachyonException te)

  bool renameFile(1: i64 fileId, 2: string dstPath)
    throws (1:FileAlreadyExistException faee, 2: FileDoesNotExistException fdnee,
      3: InvalidPathException ipe)

  void setPinned(1: i64 fileId, 2: bool pinned)
    throws (1: FileDoesNotExistException fdnee)

  bool createDirectory(1: string path, 2: bool recursive)
    throws (1: FileAlreadyExistException faee, 2: TachyonException te)

  bool free(1: i64 fileId, 2: bool recursive)
    throws (1: FileDoesNotExistException fdnee)

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
  i64 userCreateRawTable(1: string path, 2: i32 columns, 3: binary metadata)
    throws (1: FileAlreadyExistException faee, 2: InvalidPathException ipe, 3: TableColumnException tce,
      4: TachyonException te)

  i64 userGetRawTableId(1: string path)
    throws (1: InvalidPathException ipe, 2: TableDoesNotExistException tdnee)

  RawTableInfo userGetClientRawTableInfoById(1: i64 id)
    throws (1: TableDoesNotExistException tdnee)

  RawTableInfo userGetClientRawTableInfoByPath(1: string path)
    throws (1: TableDoesNotExistException tdnee, 2: InvalidPathException ipe)

  void userUpdateRawTableMetadata(1: i64 tableId, 2: binary metadata)
    throws (1: TableDoesNotExistException tdnee, 2: TachyonException te)
}




service MasterService {
  bool addCheckpoint(1: i64 workerId, 2: i32 fileId, 3: i64 length, 4: string checkpointPath)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: BlockInfoException eB)

  list<WorkerInfo> getWorkersInfo()

  list<FileInfo> liststatus(1: string path)
    throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF)

  // Services to Workers
  /**
   * Worker register and synch up capacity of Tachyon space, used space bytes and blocks in each
   * storage directory to master, the return value rv % 100,000 is really workerId, rv / 1000,000
   * is master started time. currentBlocks maps from id of storage directory to the blocks it
   * contains.
   */
  i64 worker_register(1: NetAddress workerNetAddress, 2: list<i64> totalBytesOnTiers,
       3: list<i64> usedBytesOnTiers, 4: map<i64, list<i64>> currentBlocks)
    throws (1: BlockInfoException e)

  /**
   * Heart beat between worker and master, worker update used Tachyon space in bytes, removed
   * blocks and added blocks in each storage directory by eviction and promotion to master, and
   * return the command from master to worker. addedBlockIds maps from id of storage directory
   * to the blocks added in it.
   */
  Command worker_heartbeat(1: i64 workerId, 2: list<i64> usedBytesOnTiers, 3: list<i64> removedBlockIds,
      4: map<i64, list<i64>> addedBlockIds)
    throws (1: BlockInfoException e)

  /**
   * Update information of the block newly cached to master, including used Tachyon space size in
   * bytes, the id of the storage directory in which the block is, the id of the block and the size
   * of the block in bytes.
   */
  void worker_cacheBlock(1: i64 workerId, 2: i64 usedBytesOnTier, 3: i64 storageDirId,
      4: i64 blockId, 5: i64 length)
    throws (1: FileDoesNotExistException eP, 2: BlockInfoException eB)

  set<i32> worker_getPinIdList()

  list<i32> worker_getPriorityDependencyList()

  // Services to Users
  i32 user_createDependency(1: list<string> parents, 2: list<string> children,
      3: string commandPrefix, 4: list<binary> data, 5: string comment, 6: string framework,
      7: string frameworkVersion, 8: i32 dependencyType, 9: i64 childrenBlockSizeByte)
    throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF,
      3: FileAlreadyExistException eA, 4: BlockInfoException eB, 5: TachyonException eT)

  DependencyInfo user_getClientDependencyInfo(1: i32 dependencyId)
    throws (1: DependencyDoesNotExistException e)

  void user_reportLostFile(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  void user_requestFilesInDependency(1: i32 depId)
    throws (1: DependencyDoesNotExistException e)

  i32 user_createFile(1: string path, 2: string ufsPath, 3: i64 blockSizeByte, 4: bool recursive)
    throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI, 3: BlockInfoException eB,
      4: SuspectedFileSizeException eS, 5: TachyonException eT)

  i64 user_createNewBlock(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  void user_completeFile(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  i64 user_getUserId()

  i64 user_getBlockId(1: i32 fileId, 2: i32 index)
    throws (1: FileDoesNotExistException e)

  i64 user_getCapacityBytes()

  i64 user_getUsedBytes()

  /**
   * Get local worker NetAddress
   */
  NetAddress user_getWorker(1: bool random, 2: string host)
    throws (1: NoWorkerException e)

  FileInfo getFileStatus(1: i32 fileId, 2: string path)
    throws (1: InvalidPathException eI)

  /**
   * Get block's ClientBlockInfo.
   */
  FileBlockInfo user_getClientBlockInfo(1: i64 blockId)
    throws (1: FileDoesNotExistException eF, 2: BlockInfoException eB)

  /**
   * Get file blocks info.
   */
  list<FileBlockInfo> user_getFileBlocks(1: i32 fileId, 2: string path)
    throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)

  /**
   * Delete file
   */
  bool user_delete(1: i32 fileId, 2: string path, 3: bool recursive)
    throws (1: TachyonException e)

  bool user_rename(1: i32 fileId, 2: string srcPath, 3: string dstPath)
    throws (1:FileAlreadyExistException eA, 2: FileDoesNotExistException eF,
      3: InvalidPathException eI)

  void user_setPinned(1: i32 fileId, 2: bool pinned)
    throws (1: FileDoesNotExistException e)

  bool user_mkdirs(1: string path, 2: bool recursive)
    throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI, 3: TachyonException eT)

  i32 user_createRawTable(1: string path, 2: i32 columns, 3: binary metadata)
    throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI, 3: TableColumnException eT,
      4: TachyonException eTa)

  /**
   * Return 0 if does not contain the Table, return fileId if it exists.
   */
  i32 user_getRawTableId(1: string path)
    throws (1: InvalidPathException e)

  /**
   * Get RawTable's info; Return a ClientRawTable instance with id 0 if the system does not contain
   * the table. path if valid iff id is -1.
   */
  RawTableInfo user_getClientRawTableInfo(1: i32 id, 2: string path)
    throws (1: TableDoesNotExistException eT, 2: InvalidPathException eI)

  void user_updateRawTableMetadata(1: i32 tableId, 2: binary metadata)
    throws (1: TableDoesNotExistException eT, 2: TachyonException eTa)

  string user_getUfsAddress()

  /**
   * Returns if the message was received. Intended to check if the client can still connect to the
   * master.
   */
  void user_heartbeat();

  bool user_freepath(1: i32 fileId, 2: string path, 3: bool recursive)
    throws (1: FileDoesNotExistException e)
}

service WorkerService {
  void accessBlock(1: i64 blockId)

  void addCheckpoint(1: i64 userId, 2: i32 fileId)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: FailedToCheckpointException eF, 4: BlockInfoException eB)

  bool asyncCheckpoint(1: i32 fileId)
    throws (1: TachyonException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from user
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock(1: i64 userId, 2: i64 blockId)
    throws (1: FileDoesNotExistException eP, 2: BlockInfoException eB)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock(1: i64 userId, 2: i64 blockId)

  /**
   * Used to get user's temporary folder on under file system, and the path of the user's temporary
   * folder will be returned.
   */
  string getUserUfsTempFolder(1: i64 userId)

  /**
   * Lock the file in Tachyon's space while the user is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   */
  string lockBlock(1: i64 blockId, 2: i64 userId)
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
   * storage OutOfSpaceException will be thrown, if the file is already being written by the user,
   * FileAlreadyExistException will be thrown.
   */
  string requestBlockLocation(1: i64 userId, 2: i64 blockId, 3: i64 initialBytes)
    throws (1: OutOfSpaceException eP, 2: FileAlreadyExistException eS)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace(1: i64 userId, 2: i64 blockId, 3: i64 requestBytes)
    throws (1: FileDoesNotExistException eP)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock(1: i64 blockId, 2: i64 userId)

  /**
   * Local user send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   */
  void userHeartbeat(1: i64 userId, 2: list<i64> metrics)
}
