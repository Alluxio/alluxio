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

// deprecated
struct DependencyInfo {
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
  15: i32 inMemoryPercentage
  16: i64 lastModificationTimeMs
  17: i64 ttl
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
  Register = 2,         // Ask the worker to re-register.
  Free = 3,		// Ask the worker to free files.
  Delete = 4,		// Ask the worker to delete files.
  Persist = 5,  // Ask the worker to persist a file for lineage
}

struct Command {
  1: CommandType mCommandType
  2: list<i64> mData
}

struct LineageCommand {
  1: CommandType commandType
  2: list<CheckpointFile> checkpointFiles
}

struct CheckpointFile {
  1: i64 fileId
  2: list<i64> blockIds
}

struct JobConfInfo {
  1: string outputFile
}

struct CommandLineJobInfo {
  1: string command
  2: JobConfInfo conf
}

struct LineageFileInfo {
  1: i64 id
  2: string state
  3: string underFilePath
}

struct LineageInfo {
  1: i64 id
  2: list<i64> inputFiles
  3: list<LineageFileInfo> outputFiles
  4: CommandLineJobInfo job
  5: i64 creationTimeMs
  6: list<i64> parents
  7: list<i64> children
}

exception TachyonTException {
  1: string type
  2: string message
}

exception ThriftIOException {
  1: string message
}

service BlockMasterService {
  BlockInfo getBlockInfo(1: i64 blockId) throws (1: TachyonTException e)

  i64 getCapacityBytes()

  i64 getUsedBytes()

  list<WorkerInfo> getWorkerInfoList()

  void workerCommitBlock(1: i64 workerId, 2: i64 usedBytesOnTier, 3: i32 tier, 4: i64 blockId,
      5: i64 length)
    throws (1: TachyonTException e)

  i64 workerGetWorkerId(1: NetAddress workerNetAddress)

  Command workerHeartbeat(1: i64 workerId, 2: list<i64> usedBytesOnTiers,
      3: list<i64> removedBlockIds, 4: map<i64, list<i64>> addedBlocksOnTiers)
    throws (1: TachyonTException e)

  void workerRegister(1: i64 workerId, 2: list<i64> totalBytesOnTiers,
      3: list<i64> usedBytesOnTiers, 4: map<i64, list<i64>> currentBlocksOnTiers)
    throws (1: TachyonTException e)
}

service FileSystemMasterService {
  void completeFile(1: i64 fileId) throws (1: TachyonTException e)

  bool createDirectory(1: string path, 2: bool recursive)
    throws (1: TachyonTException e)

  i64 createFile(1: string path, 2: i64 blockSizeBytes, 3: bool recursive, 4: i64 ttl)
    throws (1: TachyonTException e)

  bool deleteFile(1: i64 fileId, 2: bool recursive)
    throws (1: TachyonTException e)

  bool free(1: i64 fileId, 2: bool recursive)
    throws (1: TachyonTException e)

  FileBlockInfo getFileBlockInfo(1: i64 fileId, 2: i32 fileBlockIndex)
    throws (1: TachyonTException e)

  list<FileBlockInfo> getFileBlockInfoList(1: i64 fileId)
    throws (1: TachyonTException e)

  i64 getFileId(1: string path)
    throws (1: TachyonTException e)

  FileInfo getFileInfo(1: i64 fileId)
    throws (1: TachyonTException e)

  list<FileInfo> getFileInfoList(1: i64 fileId)
    throws (1: TachyonTException e)

  i64 getNewBlockIdForFile(1: i64 fileId)
    throws (1: TachyonTException e)

  // TODO(gene): Is this necessary?
  string getUfsAddress()

  /**
   * Loads metadata for the file identified by the given Tachyon path from UFS into Tachyon.
   */
  i64 loadFileInfoFromUfs(1: string ufsPath, 2: bool recursive) throws (1: TachyonTException e)

  /**
   * Creates a new "mount point", mounts the given UFS path in the Tachyon namespace at the given
   * path. The path should not exist and should not be nested under any existing mount point.
   */
  bool mount(1: string tachyonPath, 2: string ufsPath)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  bool persistFile(1: i64 fileId, 2: i64 length)
    throws (1: TachyonTException e)

  bool renameFile(1: i64 fileId, 2: string dstPath)
    throws (1: TachyonTException e)

  void reportLostFile(1: i64 fileId)
    throws (1: TachyonTException e)

  void setPinned(1: i64 fileId, 2: bool pinned)
    throws (1: TachyonTException e)

  /**
   * Deletes an existing "mount point", voiding the Tachyon namespace at the given path. The path
   * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
   * will be persisted before they are removed from the Tachyon namespace.
   */
  bool unmount(1: string tachyonPath) throws (1: TachyonTException e, 2: ThriftIOException ioe)

  set<i64> workerGetPinIdList()
}

service LineageMasterService {
  // for client
  i64 createLineage(1: list<string> inputFiles, 2: list<string> outputFiles, 3: CommandLineJobInfo job)
    throws (1: TachyonTException e)

  bool deleteLineage(1: i64 lineageId, 2: bool cascade)
    throws (1: TachyonTException e)

  list<LineageInfo> getLineageInfoList()

  i64 reinitializeFile(1: string path, 2: i64 blockSizeBytes, 3: i64 ttl)
    throws (1: TachyonTException e)

  void asyncCompleteFile(1: i64 fileId)
    throws (1: TachyonTException e)

  // for workers
  LineageCommand workerLineageHeartbeat(1: i64 workerId, 2: list<i64> persistedFiles)
}

service RawTableMasterService {
  i64 createRawTable(1: string path, 2: i32 columns, 3: binary metadata)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  RawTableInfo getClientRawTableInfoById(1: i64 id)
    throws (1: TachyonTException e)

  RawTableInfo getClientRawTableInfoByPath(1: string path)
    throws (1: TachyonTException e)

  i64 getRawTableId(1: string path)
    throws (1: TachyonTException e)

  void updateRawTableMetadata(1: i64 tableId, 2: binary metadata)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)
}

service WorkerService {
  void accessBlock(1: i64 blockId)

  bool asyncCheckpoint(1: i64 fileId)
    throws (1: TachyonTException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   */
  string lockBlock(1: i64 blockId, 2: i64 sessionId)
    throws (1: TachyonTException e)

  void persistFile(1: i64 fileId, 2: i64 nonce, 3: string path)
    throws (1: TachyonTException e)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   */
  bool promoteBlock(1: i64 blockId)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistsException will be thrown.
   */
  string requestBlockLocation(1: i64 sessionId, 2: i64 blockId, 3: i64 initialBytes)
    throws (1: TachyonTException e, 2: ThriftIOException ioe)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace(1: i64 sessionId, 2: i64 blockId, 3: i64 requestBytes)
    throws (1: TachyonTException e)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   */
  void sessionHeartbeat(1: i64 sessionId, 2: list<i64> metrics)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock(1: i64 blockId, 2: i64 sessionId)
}
