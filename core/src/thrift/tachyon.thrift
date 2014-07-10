namespace java tachyon.thrift

// Version 2: 0.5.0
// Version 1: before 0.5.0

struct NetAddress {
  1: string mHost
  2: i32 mPort
}

struct ClientBlockInfo {
  1: i64 blockId
  2: i64 offset
  3: i64 length
  4: list<NetAddress> locations
}

struct ClientWorkerInfo {
  1: i64 id
  2: NetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 starttimeMs
}

struct ClientFileInfo {
  1: i32 id
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
}

struct ClientDependencyInfo {
  1: i32 id
  2: list<i32> parents
  3: list<i32> children
  4: list<binary> data
}

struct ClientRawTableInfo {
  1: i32 id
  2: string name
  3: string path
  4: i32 columns
  5: binary metadata
}

enum CommandType {
  Unknown = 0,
  Nothing = 1,
  Register = 2,   // Ask the worker to re-register.
  Free = 3,				// Ask the worker to free files.
  Delete = 4,			// Ask the worker to delete files.
}

struct Command {
  1: CommandType mCommandType
  2: list<i64> mData
}

exception BlockInfoException {
  1: string message
}

exception OutOfMemoryForPinFileException {
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

service MasterService {
  bool addCheckpoint(1: i64 workerId, 2: i32 fileId, 3: i64 length, 4: string checkpointPath)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: BlockInfoException eB)

  list<ClientWorkerInfo> getWorkersInfo()

  list<ClientFileInfo> liststatus(1: string path)
    throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF)


  // Services to Workers
  /**
   * Worker register.
   * @return value rv % 100,000 is really workerId, rv / 1000,000 is master started time.
   */
  i64 worker_register(1: NetAddress workerNetAddress, 2: i64 totalBytes, 3: i64 usedBytes,
      4: list<i64> currentBlocks)
    throws (1: BlockInfoException e)

  Command worker_heartbeat(1: i64 workerId, 2: i64 usedBytes, 3: list<i64> removedBlocks)
    throws (1: BlockInfoException e)

  void worker_cacheBlock(1: i64 workerId, 2: i64 workerUsedBytes, 3: i64 blockId, 4: i64 length)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS, 3: BlockInfoException eB)

  set<i32> worker_getPinIdList()

  list<i32> worker_getPriorityDependencyList()


  // Services to Users
  i32 user_createDependency(1: list<string> parents, 2: list<string> children,
      3: string commandPrefix, 4: list<binary> data, 5: string comment, 6: string framework,
      7: string frameworkVersion, 8: i32 dependencyType, 9: i64 childrenBlockSizeByte)
    throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF,
      3: FileAlreadyExistException eA, 4: BlockInfoException eB, 5: TachyonException eT)

  ClientDependencyInfo user_getClientDependencyInfo(1: i32 dependencyId)
    throws (1: DependencyDoesNotExistException e)

  void user_reportLostFile(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  void user_requestFilesInDependency(1: i32 depId)
    throws (1: DependencyDoesNotExistException e)

  i32 user_createFile(1: string path, 2: i64 blockSizeByte)
    throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI, 3: BlockInfoException eB,
      4: TachyonException eT)

  i32 user_createFileOnCheckpoint(1: string path, 2: string checkpointPath)
    throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI,
      3: SuspectedFileSizeException eS, 4: BlockInfoException eB, 5: TachyonException eT)

  i64 user_createNewBlock(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  void user_completeFile(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  /**
   * Return -1 if does not contain the file, return fileId if it exists.
   */
  i32 user_getFileId(1: string path)
    throws (1: InvalidPathException e)

  i64 user_getUserId()

  i64 user_getBlockId(1: i32 fileId, 2: i32 index)
    throws (1: FileDoesNotExistException e)

  /**
   * Get local worker NetAddress
   */
  NetAddress user_getWorker(1: bool random, 2: string host)
    throws (1: NoWorkerException e)

  ClientFileInfo getClientFileInfoById(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  ClientFileInfo user_getClientFileInfoByPath(1: string path)
    throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)

  /**
   * Get block's ClientBlockInfo.
   */
  ClientBlockInfo user_getClientBlockInfo(1: i64 blockId)
    throws (1: FileDoesNotExistException eF, 2: BlockInfoException eB)

  /**
   * Get file locations by file Id.
   */
  list<ClientBlockInfo> user_getFileBlocksById(1: i32 fileId)
    throws (1: FileDoesNotExistException e)

  /**
   * Get file locations by path
   */
  list<ClientBlockInfo> user_getFileBlocksByPath(1: string path)
    throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)

  list<i32> user_listFiles(1: string path, 2: bool recursive)
    throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)

  list<string> user_ls(1: string path, 2: bool recursive)
    throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)

  bool user_deleteById(1: i32 fileId, 2: bool recursive) // Delete file
    throws (1: TachyonException e)

  bool user_deleteByPath(1: string path, 2: bool recursive) // Delete file
    throws (1: TachyonException e)

  void user_outOfMemoryForPinFile(1: i32 fileId)

  bool user_rename(1: string srcPath, 2: string dstPath)
    throws (1:FileAlreadyExistException eA, 2: FileDoesNotExistException eF,
      3: InvalidPathException eI)

  void user_renameTo(1: i32 fileId, 2: string dstPath)
    throws (1:FileAlreadyExistException eA, 2: FileDoesNotExistException eF,
      3: InvalidPathException eI)

  void user_setPinned(1: i32 fileId, 2: bool pinned)
    throws (1: FileDoesNotExistException e)

  bool user_mkdir(1: string path)
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
   * Get Table info by Table Id.
   */
  ClientRawTableInfo user_getClientRawTableInfoById(1: i32 tableId)
    throws (1: TableDoesNotExistException e)

  /**
   * Get Table info by path
   */
  ClientRawTableInfo user_getClientRawTableInfoByPath(1: string tablePath)
    throws (1: TableDoesNotExistException eT, 2: InvalidPathException eI)

  void user_updateRawTableMetadata(1: i32 tableId, 2: binary metadata)
    throws (1: TableDoesNotExistException eT, 2: TachyonException eTa)

  i32 user_getNumberOfFiles(1:string path)
    throws (1: FileDoesNotExistException eR, 2: InvalidPathException eI)

  string user_getUnderfsAddress()
}

service WorkerService {
  void accessBlock(1: i64 blockId)

  void addCheckpoint(1: i64 userId, 2: i32 fileId)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: FailedToCheckpointException eF, 4: BlockInfoException eB)

  bool asyncCheckpoint(1: i32 fileId)
    throws (1: TachyonException e)

  void cacheBlock(1: i64 userId, 2: i64 blockId)
    throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS,
      3: BlockInfoException eB)

  string getDataFolder()

  string getUserTempFolder(1: i64 userId)

  string getUserUnderfsTempFolder(1: i64 userId)

  void lockBlock(1: i64 blockId, 2: i64 userId) // Lock the file in memory while the user is reading it.

  void returnSpace(1: i64 userId, 2: i64 returnedBytes)

  bool requestSpace(1: i64 userId, 2: i64 requestBytes)   // Should change this to return i64, means how much space to grant.

  void unlockBlock(1: i64 blockId, 2: i64 userId) // unlock the file

  void userHeartbeat(1: i64 userId)   // Local user send heartbeat to local worker to keep its temp folder.
}
