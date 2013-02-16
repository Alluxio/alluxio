namespace java tachyon.thrift

struct NetAddress {
  1: string mHost
  2: i32 mPort
}

struct PartitionInfo {
  1: i32 mDatasetId
  2: i32 mPartitionId
  3: i32 mSizeBytes
  4: map<i64, NetAddress> mLocations
  5: bool mHasCheckpointed
  6: string mCheckpointPath
}

struct DatasetInfo {
  1: i32 mId
  2: string mPath
  3: i64 mSizeBytes
  4: i32 mNumOfPartitions
  5: list<PartitionInfo> mPartitionList
  6: bool mCache
  7: bool mPin
  8: bool mIsSubDataset
  9: i32 mParentDatasetId
}

struct RawTableInfo {
  1: i32 mId
  2: string mPath
  3: i32 mColumns
  4: i64 mSizeBytes
  5: i32 mNumOfPartitions
  6: list<i32> mColumnDatasetIdList
  7: list<PartitionInfo> mPartitionList
}

enum LogEventType {
  Undefined = 0,
  FileInfo = 1,
  RawTableInfo = 2,
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

exception TableDoesNotExistException {
  1: string message
}

service MasterService {
  // Services to Workers
  i64 worker_register(1: NetAddress workerNetAddress, 2: i64 totalBytes, 3: i64 usedBytes, 4: list<i64> currentFileList) // Returned value rv % 100,000 is really workerId, rv / 1000,000 is master started time.
  Command worker_heartbeat(1: i64 workerId, 2: i64 usedBytes, 3: list<i64> removedFileList)
  void worker_addFile(1: i64 workerId, 2: i64 workerUsedBytes, 3: i32 fileId, 4: i32 fileSizeBytes, 5: bool hasCheckpointed, 6: string checkpointPath) throws (1: FileDoesNotExistException eP, 2: SuspectedFileException eS)
  set<i32> worker_getPinList()

  // Services to Users
  i32 user_createFile(1: string filePath) throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI)
  i32 user_getFileId(1: string filePath)  // Return 0 if does not contain the dataset, return datasetId if it exists.
  i64 user_getUserId()
  NetAddress user_getLocalWorker(1: string host) throws (1: NoLocalWorkerException e) // Get local worker NetAddress
  list<NetAddress> user_getFileLocsById(1: i32 fileId) throws (1: DatasetDoesNotExistException e)        // Get file locations by file Id.
  list<NetAddress> user_getFileLocsByPath(1: string filePath) throws (1: DatasetDoesNotExistException e) // Get file locations by path
  void user_deleteFile(1: i32 fileId) throws (1: FileDoesNotExistException e) // Delete file
  void user_outOfMemoryForPinFile(1: i32 datasetId)
  void user_renameFile(1: string srcFilePath, 2: string dstFilePath) throws (1: FileDoesNotExistException e)
//  void user_setPartitionCheckpointPath(1: i32 datasetId, 2: i32 partitionId, 3: string checkpointPath) throws (1: DatasetDoesNotExistException eD, 2: PartitionDoesNotExistException eP)
  void user_unpinFile(1: i32 fileId) throws (1: FileDoesNotExistException e)   // Remove file from memory

//  i32 user_createRawTable(1: string tablePath, 2: i32 columns) throws (1: FileAlreadyExistException eR, 2: InvalidPathException eI)
//  i32 user_getRawTableId(1: string tablePath)  // Return 0 if does not contain the Table, return datasetId if it exists.
//  void user_deleteRawTable(1: i32 tabletId) throws (1: DatasetDoesNotExistException e) // Delete dataset
//  void user_renameRawTable(1: string srcTablePath, 2: string dstTablePath) throws (1: FileDoesNotExistException e)
//  RawTableInfo user_getRawTableById(1: i32 tableId) throws (1: TableDoesNotExistException e)        // Get Table info by Table Id.
//  RawTableInfo user_getRawTableByPath(1: string tablePath) throws (1: TableDoesNotExistException e) // Get Table info by path

  // cmd to scripts
  list<string> cmd_ls(1: string path)
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