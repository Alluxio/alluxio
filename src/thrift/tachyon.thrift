namespace java tachyon.thrift

struct NetAddress {
  1: string mHost
  2: i32 mPort
}

struct ClientWorkerInfo {
  1: i64 id
  2: NetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
}

struct ClientFileInfo {
  1: i32 id
  2: string name
  3: string path
  4: string checkpointPath
  5: i64 sizeBytes
  6: bool isFolder
  7: bool inMemory
  8: bool needPin
  9: bool needCache
}

struct ClientRawTableInfo {
  1: i32 id
  2: string name
  3: string path
  4: i32 columns
}

enum LogEventType {
  Undefined = 0,
  CheckpointInfo = 1,
  INode = 2,
  RawTableInfo = 3,
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
  2: binary mData
}

exception OutOfMemoryForPinFileException {
  1: string message
}

exception FileAlreadyExistException {
  1: string message
}

exception FileDoesNotExistException {
  1: string message
}

exception NoLocalWorkerException {
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

service MasterService {
  // Services to Workers
  i64 worker_register(1: NetAddress workerNetAddress, 2: i64 totalBytes, 3: i64 usedBytes, 4: list<i32> currentFiles) // Returned value rv % 100,000 is really workerId, rv / 1000,000 is master started time.
  Command worker_heartbeat(1: i64 workerId, 2: i64 usedBytes, 3: list<i32> removedFiles)
  void worker_addFile(1: i64 workerId, 2: i64 workerUsedBytes, 3: i32 fileId, 4: i32 fileSizeBytes, 5: bool hasCheckpointed, 6: string checkpointPath) throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS)
  set<i32> worker_getPinIdList()

  // Services to Users
  i32 user_createFile(1: string filePath) throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI)
  i32 user_getFileId(1: string filePath) throws (1: InvalidPathException e) // Return -1 if does not contain the dataset, return datasetId if it exists.
  i64 user_getUserId()
  NetAddress user_getLocalWorker(1: string host) throws (1: NoLocalWorkerException e) // Get local worker NetAddress
  ClientFileInfo user_getClientFileInfoById(1: i32 fileId) throws (1: FileDoesNotExistException e)
  ClientFileInfo user_getClientFileInfoByPath(1: string filePath) throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)
  list<NetAddress> user_getFileLocationsById(1: i32 fileId) throws (1: FileDoesNotExistException e)        // Get file locations by file Id.
  list<NetAddress> user_getFileLocationsByPath(1: string filePath) throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI) // Get file locations by path
  void user_deleteById(1: i32 fileId) // Delete file
  void user_deleteByPath(1: string path) throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF) // Delete file
  void user_outOfMemoryForPinFile(1: i32 datasetId)
  void user_renameFile(1: string srcFilePath, 2: string dstFilePath) throws (1: FileDoesNotExistException eF, 2: InvalidPathException eI)
//  void user_setPartitionCheckpointPath(1: i32 fileId, 2: string checkpointPath) throws (1: FileDoesNotExistException eD)
  void user_unpinFile(1: i32 fileId) throws (1: FileDoesNotExistException e)   // Remove file from memory
  i32 user_mkdir(1: string path) throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI)

  i32 user_createRawTable(1: string path, 2: i32 columns) throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI, 3: TableColumnException eT)
  i32 user_getRawTableId(1: string path) throws (1: InvalidPathException e) // Return 0 if does not contain the Table, return datasetId if it exists.
  ClientRawTableInfo user_getClientRawTableInfoById(1: i32 tableId) throws (1: TableDoesNotExistException e)        // Get Table info by Table Id.
  ClientRawTableInfo user_getClientRawTableInfoByPath(1: string tablePath) throws (1: TableDoesNotExistException eT, 2: InvalidPathException eI) // Get Table info by path
//  void user_deleteRawTable(1: i32 tabletId) throws (1: DatasetDoesNotExistException e) // Delete dataset
//  void user_renameRawTable(1: string srcTablePath, 2: string dstTablePath) throws (1: FileDoesNotExistException e)

  // cmd to scripts
  list<string> cmd_ls(1: string path) throws (1: InvalidPathException eI, 2: FileDoesNotExistException eF)
}

service WorkerService {
  void accessFile(1: i32 fileId)
  void addDoneFile(1: i64 userId, 2: i32 fileId, 3: bool writeThrough) throws (1: FileDoesNotExistException eP, 2: SuspectedFileSizeException eS, 3: FileAlreadyExistException eA)
  string getDataFolder()
  string getUserTempFolder(1: i64 userId)
  string getUserHdfsTempFolder(1: i64 userId)
  void lockFile(1: i32 fileId, 2: i64 userId) // Lock the file in memory while the user is reading it.
  void returnSpace(1: i64 userId, 2: i64 returnedBytes)
  bool requestSpace(1: i64 userId, 2: i64 requestBytes)   // Should change this to return i64, means how much space to grant.
  void unlockFile(1: i32 fileId, 2: i64 userId) // unlock the file
  void userHeartbeat(1: i64 userId)   // Local user send heartbeat to local worker to keep its temp folder.
}