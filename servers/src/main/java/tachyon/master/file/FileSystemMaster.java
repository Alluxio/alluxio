/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.collections.Pair;
import tachyon.collections.PrefixList;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockInfoException;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyCompletedException;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidFileSizeException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.PreconditionMessage;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockId;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.meta.FileSystemMasterView;
import tachyon.master.file.meta.Inode;
import tachyon.master.file.meta.InodeDirectory;
import tachyon.master.file.meta.InodeDirectoryIdGenerator;
import tachyon.master.file.meta.InodeFile;
import tachyon.master.file.meta.InodeTree;
import tachyon.master.file.meta.MountTable;
import tachyon.master.file.meta.PersistenceState;
import tachyon.master.file.meta.TTLBucket;
import tachyon.master.file.meta.TTLBucketList;
import tachyon.master.file.meta.options.CreatePathOptions;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.file.options.CreateDirectoryOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalProtoUtils;
import tachyon.proto.journal.File.AddMountPointEntry;
import tachyon.proto.journal.File.CompleteFileEntry;
import tachyon.proto.journal.File.DeleteFileEntry;
import tachyon.proto.journal.File.DeleteMountPointEntry;
import tachyon.proto.journal.File.InodeDirectoryEntry;
import tachyon.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import tachyon.proto.journal.File.InodeFileEntry;
import tachyon.proto.journal.File.InodeLastModificationTimeEntry;
import tachyon.proto.journal.File.PersistDirectoryEntry;
import tachyon.proto.journal.File.ReinitializeFileEntry;
import tachyon.proto.journal.File.RenameEntry;
import tachyon.proto.journal.File.SetStateEntry;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.proto.journal.Lineage.PersistFilesRequestEntry;
import tachyon.security.authorization.PermissionStatus;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemCommand;
import tachyon.thrift.FileSystemCommandOptions;
import tachyon.thrift.FileSystemMasterClientService;
import tachyon.thrift.FileSystemMasterWorkerService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.PersistCommandOptions;
import tachyon.thrift.PersistFile;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The master that handles all file system metadata management.
 */
public final class FileSystemMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockMaster mBlockMaster;
  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;
  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;
  /** This manages the file system mount points. */
  private final MountTable mMountTable;
  /** Map from worker to the files to persist on that worker. Used by async persistence service. */
  private final Map<Long, Set<Long>> mWorkerToAsyncPersistFiles;

  private final PrefixList mWhitelist;

  /**
   * The service that tries to check inodefiles with ttl set. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mTTLCheckerService;

  private final TTLBucketList mTTLBuckets = new TTLBucketList();

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.FILE_SYSTEM_MASTER_NAME);
  }

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param blockMaster the {@link BlockMaster} to use
   * @param journal the journal to use for tracking master operations
   */
  public FileSystemMaster(BlockMaster blockMaster, Journal journal) {
    super(journal, 2);
    mBlockMaster = blockMaster;

    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mMountTable = new MountTable();
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator, mMountTable);

    // TODO(gene): Handle default config value for whitelist.
    TachyonConf conf = MasterContext.getConf();
    mWhitelist = new PrefixList(conf.getList(Constants.MASTER_WHITELIST, ","));

    mWorkerToAsyncPersistFiles = Maps.newHashMap();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientService.Processor<FileSystemMasterClientServiceHandler>(
            new FileSystemMasterClientServiceHandler(this)));
    services.put(
        Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<FileSystemMasterWorkerServiceHandler>(
            new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    if (innerEntry instanceof InodeFileEntry || innerEntry instanceof InodeDirectoryEntry) {
      mInodeTree.addInodeFromJournal(entry);
    } else if (innerEntry instanceof InodeLastModificationTimeEntry) {
      InodeLastModificationTimeEntry modTimeEntry = (InodeLastModificationTimeEntry) innerEntry;
      try {
        Inode inode = mInodeTree.getInodeById(modTimeEntry.getId());
        inode.setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof PersistDirectoryEntry) {
      PersistDirectoryEntry typedEntry = (PersistDirectoryEntry) innerEntry;
      try {
        Inode inode = mInodeTree.getInodeById(typedEntry.getId());
        inode.setPersistenceState(PersistenceState.PERSISTED);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof CompleteFileEntry) {
      try {
        completeFileFromEntry((CompleteFileEntry) innerEntry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      } catch (InvalidFileSizeException e) {
        throw new RuntimeException(e);
      } catch (FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof SetStateEntry) {
      try {
        setStateFromEntry((SetStateEntry) innerEntry);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof DeleteFileEntry) {
      deleteFileFromEntry((DeleteFileEntry) innerEntry);
    } else if (innerEntry instanceof RenameEntry) {
      renameFromEntry((RenameEntry) innerEntry);
    } else if (innerEntry instanceof InodeDirectoryIdGeneratorEntry) {
      mDirectoryIdGenerator.initFromJournalEntry((InodeDirectoryIdGeneratorEntry) innerEntry);
    } else if (innerEntry instanceof ReinitializeFileEntry) {
      resetBlockFileFromEntry((ReinitializeFileEntry) innerEntry);
    } else if (innerEntry instanceof AddMountPointEntry) {
      try {
        mountFromEntry((AddMountPointEntry) innerEntry);
      } catch (FileAlreadyExistsException e) {
        throw new RuntimeException(e);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof DeleteMountPointEntry) {
      try {
        unmountFromEntry((DeleteMountPointEntry) innerEntry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof PersistFilesRequestEntry) {
      try {
        setPersistingState(((PersistFilesRequestEntry) innerEntry).getFileIdsList());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(innerEntry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    mInodeTree.streamToJournalCheckpoint(outputStream);
    outputStream.writeEntry(mDirectoryIdGenerator.toJournalEntry());
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    if (isLeader) {
      // Only initialize root when isLeader because when initializing root, BlockMaster needs to
      // write journal entry, if it is not leader, BlockMaster won't have a writable journal.
      // If it is standby, it should be able to load the inode tree from leader's checkpoint.
      mInodeTree.initializeRoot(PermissionStatus.get(MasterContext.getConf(), false));
      String defaultUFS = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
      try {
        mMountTable.add(new TachyonURI(MountTable.ROOT), new TachyonURI(defaultUFS));
      } catch (FileAlreadyExistsException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      } catch (InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
    }
    // Call super.start after mInodeTree is initialized because mInodeTree is needed to write
    // a journal entry during super.start. Call super.start before calling
    // getExecutorService() because the super.start initializes the executor service.
    super.start(isLeader);
    if (isLeader) {
      mTTLCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTTLCheckExecutor(),
              MasterContext.getConf().getInt(Constants.MASTER_TTLCHECKER_INTERVAL_MS)));
    }
  }

  /**
   * Whether the filesystem contains a directory with the id. Called by internal masters.
   *
   * @param id id of the directory
   * @return true if there is a directory with the id, false otherwise
   */
  public boolean isDirectory(long id) {
    synchronized (mInodeTree) {
      Inode inode;
      try {
        inode = mInodeTree.getInodeById(id);
      } catch (FileDoesNotExistException fne) {
        return false;
      }
      return inode.isDirectory();
    }
  }

  /**
   * Returns the file id for a given path. Called via RPC, as well as internal masters. If the given
   * path does not exist in Tachyon, the method attempts to load it from UFS.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   */
  public long getFileId(TachyonURI path) {
    synchronized (mInodeTree) {
      Inode inode;
      try {
        inode = mInodeTree.getInodeByPath(path);
      } catch (InvalidPathException e) {
        try {
          return loadMetadata(path, true);
        } catch (Exception e2) {
          return IdUtils.INVALID_FILE_ID;
        }
      }
      return inode.getId();
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path. Called via RPC, as well as internal masters.
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   */
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path. Called via RPC, as well as internal masters.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   */
  public FileInfo getFileInfo(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * Returns the persistence state of a given file.
   *
   * @param fileId the file id
   * @return the persistence state
   * @throws FileDoesNotExistException if the file does not exist
   */
  public PersistenceState getPersistenceState(long fileId)
      throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      return inode.getPersistenceState();
    }
  }

  private FileInfo getFileInfoInternal(Inode inode) throws FileDoesNotExistException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    FileInfo fileInfo = inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    fileInfo.inMemoryPercentage = getInMemoryPercentage(inode);
    TachyonURI path = mInodeTree.getPath(inode);
    TachyonURI resolvedPath;
    try {
      resolvedPath = mMountTable.resolve(path);
    } catch (InvalidPathException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    }
    // Only set the UFS path if the path is nested under a mount point.
    if (!path.equals(resolvedPath)) {
      fileInfo.setUfsPath(resolvedPath.toString());
    }
    MasterContext.getMasterSource().incFileInfosGot(1);
    return fileInfo;
  }

  /**
   * Returns a list {@link FileInfo} for a given file id. If the given file id is a file, the list
   * only contains a single object. If it is a directory, the resulting list contains all direct
   * children of the directory. Called via RPC, as well as internal masters.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @return the list of {@link FileInfo}s
   * @throws FileDoesNotExistException if the file does not exist
   */
  public List<FileInfo> getFileInfoList(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      // getFileInfoList should load from ufs if the file does not exist
      getFileId(path);
      Inode inode = mInodeTree.getInodeByPath(path);

      List<FileInfo> ret = new ArrayList<FileInfo>();
      if (inode.isDirectory()) {
        for (Inode child : ((InodeDirectory) inode).getChildren()) {
          ret.add(getFileInfoInternal(child));
        }
      } else {
        ret.add(getFileInfoInternal(inode));
      }
      MasterContext.getMasterSource().incFileInfosGot(ret.size());
      return ret;
    }
  }

  /**
   * @return a read-only view of the file system master
   */
  public FileSystemMasterView getFileSystemMasterView() {
    return new FileSystemMasterView(this);
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to. Called via RPC.
   *
   * @param path the file path to complete
   * @param options the method options
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   */
  public void completeFile(TachyonURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException {
    MasterContext.getMasterSource().incCompleteFileOps(1);
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      Inode inode = mInodeTree.getInodeByPath(path);
      long fileId = inode.getId();
      if (!inode.isFile()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
      }

      InodeFile fileInode = (InodeFile) inode;
      List<Long> blockIdList = fileInode.getBlockIds();
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
      if (!fileInode.isPersisted() && blockInfoList.size() != blockIdList.size()) {
        throw new BlockInfoException("Cannot complete a file without all the blocks committed");
      }

      // Iterate over all file blocks committed to Tachyon, computing the length and verify that all
      // the blocks (except the last one) is the same size as the file block size.
      long inMemoryLength = 0;
      long fileBlockSize = fileInode.getBlockSizeBytes();
      for (int i = 0; i < blockInfoList.size(); i ++) {
        BlockInfo blockInfo = blockInfoList.get(i);
        inMemoryLength += blockInfo.getLength();
        if (i < blockInfoList.size() - 1 && blockInfo.getLength() != fileBlockSize) {
          throw new BlockInfoException(
              "Block index " + i + " has a block size smaller than the file block size ("
                  + fileInode.getBlockSizeBytes() + ")");
        }
      }

      // If the file is persisted, its length is determined by UFS. Otherwise, its length is
      // determined by its memory footprint.
      long length = fileInode.isPersisted() ? options.getUfsLength() : inMemoryLength;

      completeFileInternal(fileInode.getBlockIds(), fileId, length, opTimeMs);
      CompleteFileEntry completeFileEntry = CompleteFileEntry.newBuilder()
          .addAllBlockIds(fileInode.getBlockIds())
          .setId(fileId)
          .setLength(length)
          .setOpTimeMs(opTimeMs)
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setCompleteFile(completeFileEntry).build());
      flushJournal();
    }
  }

  void completeFileInternal(List<Long> blockIds, long fileId, long length, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    InodeFile inode = (InodeFile) mInodeTree.getInodeById(fileId);
    inode.setBlockIds(blockIds);
    inode.setLastModificationTimeMs(opTimeMs);
    inode.complete(length);

    if (inode.isPersisted()) {
      // Commit all the file blocks (without locations) so the metadata for the block exists.
      long currLength = length;
      for (long blockId : inode.getBlockIds()) {
        long blockSize = Math.min(currLength, inode.getBlockSizeBytes());
        mBlockMaster.commitBlockInUFS(blockId, blockSize);
        currLength -= blockSize;
      }
    }
    MasterContext.getMasterSource().incFilesCompleted(1);
  }

  private void completeFileFromEntry(CompleteFileEntry entry)
      throws InvalidPathException, InvalidFileSizeException, FileAlreadyCompletedException {
    try {
      completeFileInternal(entry.getBlockIdsList(), entry.getId(), entry.getLength(),
          entry.getOpTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }

  /**
   * Creates a file (not a directory) for a given path. Called via RPC.
   *
   * @param path the file to create
   * @param options method options
   * @return the file id of the create file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information in encountered
   */
  public long create(TachyonURI path, CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException {
    MasterContext.getMasterSource().incCreateFileOps(1);
    synchronized (mInodeTree) {
      InodeTree.CreatePathResult createResult = createInternal(path, options);
      List<Inode> created = createResult.getCreated();

      writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
      journalCreatePathResult(createResult);
      flushJournal();
      return created.get(created.size() - 1).getId();
    }
  }

  InodeTree.CreatePathResult createInternal(TachyonURI path, CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    CreatePathOptions createPathOptions = new CreatePathOptions.Builder(MasterContext.getConf())
        .setBlockSizeBytes(options.getBlockSizeBytes()).setDirectory(false)
        .setOperationTimeMs(options.getOperationTimeMs()).setPersisted(options.isPersisted())
        .setRecursive(options.isRecursive()).setTTL(options.getTTL())
        .setPermissionStatus(PermissionStatus.get(MasterContext.getConf(), true)).build();
    InodeTree.CreatePathResult createResult = mInodeTree.createPath(path, createPathOptions);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(path.toString())) {
      inode.setCacheable(true);
    }

    mTTLBuckets.insert(inode);

    MasterContext.getMasterSource().incFilesCreated(1);
    MasterContext.getMasterSource().incDirectoriesCreated(created.size() - 1);
    return createResult;
  }

  /**
   * Reinitializes the blocks of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   */
  public long reinitializeFile(TachyonURI path, long blockSizeBytes, long ttl)
      throws InvalidPathException {
    synchronized (mInodeTree) {
      long id = mInodeTree.reinitializeFile(path, blockSizeBytes, ttl);
      ReinitializeFileEntry reinitializeFile = ReinitializeFileEntry.newBuilder()
          .setPath(path.getPath())
          .setBlockSizeBytes(blockSizeBytes)
          .setTtl(ttl)
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setReinitializeFile(reinitializeFile).build());
      flushJournal();
      return id;
    }
  }

  private void resetBlockFileFromEntry(ReinitializeFileEntry entry) {
    try {
      mInodeTree.reinitializeFile(new TachyonURI(entry.getPath()), entry.getBlockSizeBytes(),
          entry.getTtl());
    } catch (InvalidPathException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the next block id for a given file id. Called via RPC.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public long getNewBlockIdForFile(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetNewBlockOps(1);
    Inode inode;
    synchronized (mInodeTree) {
      inode = mInodeTree.getInodeByPath(path);
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
    MasterContext.getMasterSource().incNewBlocksGot(1);
    return ((InodeFile) inode).getNewBlockId();
  }

  /**
   * Get the total number of files and directories.
   *
   * @return the number of files and directories
   */
  public int getNumberOfPaths() {
    synchronized (mInodeTree) {
      return mInodeTree.getSize();
    }
  }

  /**
   * Get the number of pinned files and directories.
   *
   * @return the number of pinned files and directories
   */
  public int getNumberOfPinnedFiles() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinnedSize();
    }
  }

  /**
   * Deletes a given path. Called via RPC.
   *
   * @param path the path to delete
   * @param recursive if true, will delete all its children
   * @return true if the file was deleted, false otherwise
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  public boolean deleteFile(TachyonURI path, boolean recursive) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException {
    MasterContext.getMasterSource().incDeletePathOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      long fileId = inode.getId();
      long opTimeMs = System.currentTimeMillis();
      boolean ret = deleteFileInternal(fileId, recursive, false, opTimeMs);
      DeleteFileEntry deleteFile = DeleteFileEntry.newBuilder()
          .setId(fileId)
          .setRecursive(recursive)
          .setOpTimeMs(opTimeMs)
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build());
      flushJournal();
      return ret;
    }
  }

  private void deleteFileFromEntry(DeleteFileEntry entry) {
    MasterContext.getMasterSource().incDeletePathOps(1);
    try {
      deleteFileInternal(entry.getId(), entry.getRecursive(), true, entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for avoiding {@link DirectoryNotEmptyException} when calling
   * {@link #deleteFileInternal(long, boolean, boolean, long)}.
   *
   * @param fileId the file id
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return if the file is successfully deleted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   */
  boolean deleteFileRecursiveInternal(long fileId, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, IOException {
    try {
      return deleteFileInternal(fileId, true, replayed, opTimeMs);
    } catch (DirectoryNotEmptyException e) {
      throw new IllegalStateException(
          "deleteFileInternal should never throw DirectoryNotEmptyException when recursive is true",
          e);
    }
  }

  /**
   * Implements file deletion.
   *
   * @param fileId the file id
   * @param recursive if the file id identifies a directory, this flag specifies whether the
   *        directory content should be deleted recursively
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return true if the file is successfully deleted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  boolean deleteFileInternal(long fileId, boolean recursive, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, IOException, DirectoryNotEmptyException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    //
    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Tachyon and UFS.
    Inode inode = mInodeTree.getInodeById(fileId);
    if (inode == null) {
      return true;
    }
    if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
      // true
      throw new DirectoryNotEmptyException(ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE,
          inode.getName());
    }
    if (mInodeTree.isRootId(inode.getId())) {
      // The root cannot be deleted.
      return false;
    }

    List<Inode> delInodes = new ArrayList<Inode>();
    delInodes.add(inode);
    if (inode.isDirectory()) {
      delInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
    }

    // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
    // file, we deal with the checkpoints and blocks as well.
    for (int i = delInodes.size() - 1; i >= 0; i --) {
      Inode delInode = delInodes.get(i);

      // TODO(jiri): What should the Tachyon behavior be when a UFS delete operation fails?
      // Currently, it will result in an inconsistency between Tachyon and UFS.
      if (!replayed && delInode.isPersisted()) {
        // Delete the file in the under file system.
        try {
          String ufsPath = mMountTable.resolve(mInodeTree.getPath(delInode)).toString();
          UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
          if (!ufs.exists(ufsPath)) {
            LOG.warn("File does not exist the underfs: {}", ufsPath);
          } else if (!ufs.delete(ufsPath, true)) {
            LOG.error("Failed to delete {}", ufsPath);
            return false;
          }
        } catch (InvalidPathException e) {
          LOG.warn(e.getMessage());
        }
      }

      if (delInode.isFile()) {
        // Remove corresponding blocks from workers.
        mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds());
      }

      mInodeTree.deleteInode(delInode, opTimeMs);
    }
    MasterContext.getMasterSource().incPathsDeleted(delInodes.size());
    return true;
  }

  /**
   * Returns the {@link FileBlockInfo} for given file and block index. Called via RPC.
   *
   * @param fileId the file id to get the info for
   * @param fileBlockIndex the block index of the file to get the block info for
   * @return the {@link FileBlockInfo} for the file and block index
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if the block size is invalid
   */
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileBlockInfoOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException(
            ExceptionMessage.FILEID_MUST_BE_FILE.getMessage(fileId));
      }
      InodeFile file = (InodeFile) inode;
      List<Long> blockIdList = new ArrayList<Long>(1);
      blockIdList.add(file.getBlockIdByIndex(fileBlockIndex));
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
      if (blockInfoList.size() != 1) {
        throw new BlockInfoException(
            "FileId " + fileId + " BlockIndex " + fileBlockIndex + " is not a valid block.");
      }
      FileBlockInfo blockInfo = generateFileBlockInfo(file, blockInfoList.get(0));
      MasterContext.getMasterSource().incFileBlockInfosGot(1);
      return blockInfo;
    }
  }

  /**
   * Returns all the {@link FileBlockInfo} of the given file. Called via RPC, and internal masters.
   *
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public List<FileBlockInfo> getFileBlockInfoList(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileBlockInfoOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
      }
      InodeFile file = (InodeFile) inode;
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(file.getBlockIds());

      List<FileBlockInfo> ret = new ArrayList<FileBlockInfo>();
      for (BlockInfo blockInfo : blockInfoList) {
        ret.add(generateFileBlockInfo(file, blockInfo));
      }
      MasterContext.getMasterSource().incFileBlockInfosGot(ret.size());
      return ret;
    }
  }

  /**
   * Generates a {@link FileBlockInfo} object from internal metadata. This adds file information to
   * the block, such as the file offset, and additional UFS locations for the block.
   *
   * @param file the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   */
  private FileBlockInfo generateFileBlockInfo(InodeFile file, BlockInfo blockInfo)
      throws InvalidPathException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.blockInfo = blockInfo;
    fileBlockInfo.ufsLocations = new ArrayList<NetAddress>();

    // The sequence number part of the block id is the block index.
    fileBlockInfo.offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.blockId);

    if (fileBlockInfo.blockInfo.locations.isEmpty() && file.isPersisted()) {
      // No tachyon locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      String ufsPath = mMountTable.resolve(mInodeTree.getPath(file)).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
      List<String> locs;
      try {
        locs = ufs.getFileLocations(ufsPath, fileBlockInfo.offset);
      } catch (IOException e) {
        return fileBlockInfo;
      }
      if (locs != null) {
        for (String loc : locs) {
          String resolvedHost = loc;
          int resolvedPort = -1;
          try {
            String[] ipport = loc.split(":");
            if (ipport.length == 2) {
              resolvedHost = ipport[0];
              resolvedPort = Integer.parseInt(ipport[1]);
            }
          } catch (NumberFormatException nfe) {
            continue;
          }
          // The resolved port is the data transfer port not the rpc port
          fileBlockInfo.ufsLocations.add(new NetAddress(resolvedHost, -1, resolvedPort, -1));
        }
      }
    }
    return fileBlockInfo;
  }

  /**
   * Returns whether the inodeFile is fully in memory or not. The file is fully in memory only if
   * all the blocks of the file are in memory, in other words, the in memory percentage is 100.
   *
   * @return true if the file is fully in memory, false otherwise
   */
  private boolean isFullyInMemory(InodeFile inode) {
    return getInMemoryPercentage(inode) == 100;
  }

  /**
   * Gets absolute paths of all in memory files. Called by the web ui.
   *
   * @return absolute paths of all in memory files
   */
  public List<TachyonURI> getInMemoryFiles() {
    List<TachyonURI> ret = new ArrayList<TachyonURI>();
    Queue<Pair<InodeDirectory, TachyonURI>> nodesQueue =
        new LinkedList<Pair<InodeDirectory, TachyonURI>>();
    synchronized (mInodeTree) {
      // TODO(yupeng): Verify we want to use absolute path.
      nodesQueue.add(new Pair<InodeDirectory, TachyonURI>(mInodeTree.getRoot(),
          new TachyonURI(TachyonURI.SEPARATOR)));
      while (!nodesQueue.isEmpty()) {
        Pair<InodeDirectory, TachyonURI> pair = nodesQueue.poll();
        InodeDirectory directory = pair.getFirst();
        TachyonURI curUri = pair.getSecond();

        Set<Inode> children = directory.getChildren();
        for (Inode inode : children) {
          TachyonURI newUri = curUri.join(inode.getName());
          if (inode.isDirectory()) {
            nodesQueue.add(new Pair<InodeDirectory, TachyonURI>((InodeDirectory) inode, newUri));
          } else if (isFullyInMemory((InodeFile) inode)) {
            ret.add(newUri);
          }
        }
      }
    }
    return ret;
  }

  /**
   * Get the in-memory percentage of an Inode. For a file that has all blocks in memory, it returns
   * 100; for a file that has no block in memory, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in memory percentage
   */
  private int getInMemoryPercentage(Inode inode) {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = (InodeFile) inode;

    long length = inodeFile.getLength();
    if (length == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (BlockInfo info : mBlockMaster.getBlockInfoList(inodeFile.getBlockIds())) {
      if (isInTopStorageTier(info)) {
        inMemoryLength += info.getLength();
      }
    }
    return (int) (inMemoryLength * 100 / length);
  }

  /**
   * @return true if the given block is in the top storage level in some worker, false otherwise
   */
  private boolean isInTopStorageTier(BlockInfo blockInfo) {
    for (BlockLocation location : blockInfo.getLocations()) {
      if (mBlockMaster.getGlobalStorageTierAssoc().getOrdinal(location.getTierAlias()) == 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates a directory for a given path. Called via RPC, and internal masters.
   *
   * @param path the path of the directory
   * @param options method options
   * @return an {@link tachyon.master.file.meta.InodeTree.CreatePathResult} representing the
   *         modified inodes and created inodes during path creation
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath(TachyonURI, CreatePathOptions)} for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException
   */
  public InodeTree.CreatePathResult mkdir(TachyonURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException {
    LOG.debug("mkdir {} ", path);
    MasterContext.getMasterSource().incCreateDirectoriesOps(1);
    synchronized (mInodeTree) {
      try {
        CreatePathOptions createPathOptions = new CreatePathOptions.Builder(MasterContext.getConf())
            .setAllowExists(options.isAllowExists())
            .setDirectory(true)
            .setPersisted(options.isPersisted())
            .setRecursive(options.isRecursive())
            .setOperationTimeMs(options.getOperationTimeMs())
            .setPermissionStatus(PermissionStatus.get(MasterContext.getConf(), true))
            .build();
        InodeTree.CreatePathResult createResult = mInodeTree.createPath(path, createPathOptions);

        LOG.debug("writing journal entry for mkdir {}", path);
        writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
        journalCreatePathResult(createResult);
        flushJournal();
        LOG.debug("flushed journal for mkdir {}", path);
        MasterContext.getMasterSource().incDirectoriesCreated(1);
        return createResult;
      } catch (BlockInfoException bie) {
        // Since we are creating a directory, the block size is ignored, no such exception should
        // happen.
        Throwables.propagate(bie);
      }
    }
    return null;
  }

  /**
   * Journals the {@link InodeTree.CreatePathResult}. This does not flush the journal.
   * Synchronization is required outside of this method.
   *
   * @param createResult the {@link InodeTree.CreatePathResult} to journal
   */
  private void journalCreatePathResult(InodeTree.CreatePathResult createResult) {
    for (Inode inode : createResult.getModified()) {
      InodeLastModificationTimeEntry inodeLastModificationTime =
          InodeLastModificationTimeEntry.newBuilder()
          .setId(inode.getId())
          .setLastModificationTimeMs(inode.getLastModificationTimeMs())
          .build();
      writeJournalEntry(JournalEntry.newBuilder()
          .setInodeLastModificationTime(inodeLastModificationTime).build());
    }
    for (Inode inode : createResult.getCreated()) {
      writeJournalEntry(inode.toJournalEntry());
    }
    for (Inode inode : createResult.getPersisted()) {
      PersistDirectoryEntry persistDirectory = PersistDirectoryEntry.newBuilder()
          .setId(inode.getId())
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
    }
  }

  /**
   * Renames a file to a destination. Called via RPC.
   *
   * @param srcPath the source path to rename
   * @param dstPath the destination path to rename the file to
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   */
  public void rename(TachyonURI srcPath, TachyonURI dstPath) throws FileAlreadyExistsException,
      FileDoesNotExistException, InvalidPathException, IOException {
    MasterContext.getMasterSource().incRenamePathOps(1);
    synchronized (mInodeTree) {
      Inode srcInode = mInodeTree.getInodeByPath(srcPath);
      // Renaming path to itself is a no-op.
      if (srcPath.equals(dstPath)) {
        return;
      }
      // Renaming the root is not allowed.
      if (srcPath.isRoot() || dstPath.isRoot()) {
        throw new InvalidPathException(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage());
      }
      // Renaming across mount points is not allowed.
      String srcMount = mMountTable.getMountPoint(srcPath);
      String dstMount = mMountTable.getMountPoint(dstPath);
      if ((srcMount == null && dstMount != null) || (srcMount != null && dstMount == null)
          || (srcMount != null && dstMount != null && !srcMount.equals(dstMount))) {
        throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_ACROSS_MOUNTS.getMessage(
            srcPath, dstPath));
      }
      // Renaming onto a mount point is not allowed.
      if (mMountTable.isMountPoint(dstPath)) {
        throw new InvalidPathException(
            ExceptionMessage.RENAME_CANNOT_BE_ONTO_MOUNT_POINT.getMessage(dstPath));
      }
      // Renaming a path to one of its subpaths is not allowed. Check for that, by making sure
      // srcComponents isn't a prefix of dstComponents.
      if (PathUtils.hasPrefix(dstPath.getPath(), srcPath.getPath())) {
        throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_SUBPATH.getMessage(
            srcPath, dstPath));
      }

      TachyonURI dstParentURI = dstPath.getParent();

      // Get the inodes of the src and dst parents.
      Inode srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
      if (!srcParentInode.isDirectory()) {
        throw new InvalidPathException(
            ExceptionMessage.FILE_MUST_HAVE_VALID_PARENT.getMessage(srcPath));
      }
      Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
      if (!dstParentInode.isDirectory()) {
        throw new InvalidPathException(
            ExceptionMessage.FILE_MUST_HAVE_VALID_PARENT.getMessage(dstPath));
      }

      // Make sure destination path does not exist
      InodeDirectory dstParentDirectory = (InodeDirectory) dstParentInode;
      String[] dstComponents = PathUtils.getPathComponents(dstPath.getPath());
      if (dstParentDirectory.getChild(dstComponents[dstComponents.length - 1]) != null) {
        throw new FileAlreadyExistsException(
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(dstPath));
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      long opTimeMs = System.currentTimeMillis();
      renameInternal(srcInode.getId(), dstPath, false, opTimeMs);

      RenameEntry rename = RenameEntry.newBuilder()
          .setId(srcInode.getId())
          .setDstPath(dstPath.getPath())
          .setOpTimeMs(opTimeMs)
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setRename(rename).build());
      flushJournal();

      LOG.debug("Renamed {} to {}", srcPath, dstPath);
    }
  }

  /**
   * Implements renaming.
   *
   * @param fileId the file id of the rename source
   * @param dstPath the path to the rename destionation
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return true if the operation was successful and false otherwise
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  void renameInternal(long fileId, TachyonURI dstPath, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    Inode srcInode = mInodeTree.getInodeById(fileId);
    TachyonURI srcPath = mInodeTree.getPath(srcInode);
    LOG.debug("Renaming {} to {}", srcPath, dstPath);

    // If the source file is persisted, rename it in the UFS.
    FileInfo fileInfo = getFileInfoInternal(srcInode);
    if (!replayed && fileInfo.isPersisted) {
      String ufsSrcPath = mMountTable.resolve(srcPath).toString();
      String ufsDstPath = mMountTable.resolve(dstPath).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsSrcPath, MasterContext.getConf());
      String parentPath = new TachyonURI(ufsDstPath).getParent().toString();
      if (!ufs.exists(parentPath) && !ufs.mkdirs(parentPath, true)) {
        throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(parentPath));
      }
      if (!ufs.rename(ufsSrcPath, ufsDstPath)) {
        throw new IOException(
            ExceptionMessage.FAILED_UFS_RENAME.getMessage(ufsSrcPath, ufsDstPath));
      }
    }

    // TODO(jiri): A crash between now and the time the rename operation is journaled will result in
    // an inconsistency between Tachyon and UFS.
    Inode srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
    TachyonURI dstParentURI = dstPath.getParent();
    Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
    ((InodeDirectory) srcParentInode).removeChild(srcInode);
    srcParentInode.setLastModificationTimeMs(opTimeMs);
    srcInode.setParentId(dstParentInode.getId());
    srcInode.setName(dstPath.getName());
    ((InodeDirectory) dstParentInode).addChild(srcInode);
    dstParentInode.setLastModificationTimeMs(opTimeMs);
    MasterContext.getMasterSource().incPathsRenamed(1);
    propagatePersisted(srcInode, replayed);
  }

  private void renameFromEntry(RenameEntry entry) {
    MasterContext.getMasterSource().incRenamePathOps(1);
    try {
      renameInternal(entry.getId(), new TachyonURI(entry.getDstPath()), true,
          entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * @param inode the inode to start the propagation at
   * @param replayed whether the invocation is a result of replaying the journal
   * @throws FileDoesNotExistException if a non-existent file is encountered
   */
  private void propagatePersisted(Inode inode, boolean replayed)
      throws FileDoesNotExistException {
    if (!inode.isPersisted()) {
      return;
    }
    Inode handle = inode;
    while (handle.getParentId() != InodeTree.NO_PARENT) {
      handle = mInodeTree.getInodeById(handle.getParentId());
      TachyonURI path = mInodeTree.getPath(handle);
      if (mMountTable.isMountPoint(path)) {
        // Stop propagating the persisted status at mount points.
        break;
      }
      if (handle.isPersisted()) {
        // Stop if a persisted directory is encountered.
        break;
      }
      handle.setPersistenceState(PersistenceState.PERSISTED);
      if (!replayed) {
        PersistDirectoryEntry persistDirectory = PersistDirectoryEntry.newBuilder()
            .setId(inode.getId())
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
      }
    }
  }

  /**
   * Frees or evicts all of the blocks of the file from tachyon storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed. Called
   * via RPC.
   *
   * @param path the path to free
   * @param recursive if true, and the file is a directory, all descendants will be freed
   * @return true if the file was freed
   * @throws FileDoesNotExistException if the file does not exist
   */
  public boolean free(TachyonURI path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incFreeFileOps(1);
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);

      if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
        // true
        return false;
      }

      List<Inode> freeInodes = new ArrayList<Inode>();
      freeInodes.add(inode);
      if (inode.isDirectory()) {
        freeInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
      }

      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i --) {
        Inode freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds());
        }
      }
      MasterContext.getMasterSource().incFilesFreed(freeInodes.size());
    }
    return true;
  }

  /**
   * Gets the path of a file with the given id. Called by the internal web ui.
   *
   * @param fileId The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  public TachyonURI getPath(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      return mInodeTree.getPath(mInodeTree.getInodeById(fileId));
    }
  }

  /**
   * @return the set of inode ids which are pinned. Called via RPC
   */
  public Set<Long> getPinIdList() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinIdSet();
    }
  }

  /**
   * @return the ufs address for this master
   */
  public String getUfsAddress() {
    return MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
  }

  /**
   * @return the white list. Called by the internal web ui
   */
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  /**
   * @return all the files lost on the workers
   */
  public List<Long> getLostFiles() {
    Set<Long> lostFiles = Sets.newHashSet();
    for (long blockId : mBlockMaster.getLostBlocks()) {
      // the file id is the container id of the block id
      long containerId = BlockId.getContainerId(blockId);
      long fileId = IdUtils.createFileId(containerId);
      lostFiles.add(fileId);
    }
    return new ArrayList<Long>(lostFiles);
  }

  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory {}", inode);
        return;
      }

      List<Long> blockIds = Lists.newArrayList();
      try {
        for (FileBlockInfo fileBlockInfo : getFileBlockInfoList(getPath(fileId))) {
          blockIds.add(fileBlockInfo.blockInfo.blockId);
        }
      } catch (InvalidPathException e) {
        LOG.info("Failed to get file info {}", fileId, e);
      }
      mBlockMaster.reportLostBlocks(blockIds);
      LOG.info("Reported file loss of blocks {}. Tachyon will recompute it: {}", blockIds, fileId);
    }
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Tachyon.
   *
   * @param path the path for which metadata should be loaded
   * @param recursive whether parent directories should be created if they do not already exist
   * @return the file id of the loaded path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileAlreadyExistsException if the object to be loaded already exists
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws IOException if an I/O error occurs
   */
  // TODO(jiri): Make it possible to load UFS objects recursively.
  public long loadMetadata(TachyonURI path, boolean recursive)
      throws BlockInfoException, FileAlreadyExistsException, FileDoesNotExistException,
      InvalidPathException, InvalidFileSizeException, FileAlreadyCompletedException, IOException {
    TachyonURI ufsPath;
    synchronized (mInodeTree) {
      ufsPath = mMountTable.resolve(path);
    }
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath.toString(), MasterContext.getConf());
    try {
      if (!ufs.exists(ufsPath.getPath())) {
        throw new FileDoesNotExistException(
            ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path.getPath()));
      }
      if (ufs.isFile(ufsPath.getPath())) {
        long ufsBlockSizeByte = ufs.getBlockSizeByte(ufsPath.toString());
        long ufsLength = ufs.getFileSize(ufsPath.toString());
        // Metadata loaded from UFS has no TTL set.
        CreateFileOptions createFileOptions = new CreateFileOptions.Builder(MasterContext.getConf())
            .setBlockSizeBytes(ufsBlockSizeByte)
            .setRecursive(recursive)
            .setPersisted(true)
            .build();
        long fileId = create(path, createFileOptions);
        CompleteFileOptions completeOptions =
            new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(ufsLength)
                .build();
        completeFile(path, completeOptions);
        return fileId;
      } else {
        return loadMetadataDirectory(path, recursive);
      }
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Tachyon. This does
   * not actually require looking at the UFS path.
   *
   * @param path the path for which metadata should be loaded
   * @param recursive whether parent directories should be created if they do not already exist
   * @return the file id of the loaded directory
   * @throws FileAlreadyExistsException if the object to be loaded already exists
   * @throws InvalidPathException if invalid path is encountered
   * @throws IOException if an I/O error occurs
   */
  private long loadMetadataDirectory(TachyonURI path, boolean recursive)
      throws IOException, FileAlreadyExistsException, InvalidPathException {
    CreateDirectoryOptions options =
        new CreateDirectoryOptions.Builder(MasterContext.getConf()).setRecursive(recursive)
            .setPersisted(true).build();
    InodeTree.CreatePathResult result = mkdir(path, options);
    List<Inode> inodes = null;
    if (result.getCreated().size() > 0) {
      inodes = result.getCreated();
    } else if (result.getPersisted().size() > 0) {
      inodes = result.getPersisted();
    } else if (result.getModified().size() > 0) {
      inodes = result.getModified();
    }
    if (inodes == null) {
      throw new FileAlreadyExistsException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
    }
    return inodes.get(inodes.size() - 1).getId();
  }

  /**
   * Mounts a UFS path onto a Tachyon path.
   *
   * @param tachyonPath the Tachyon path to mount to
   * @param ufsPath the UFS path to mount
   * @throws FileAlreadyExistsException if the path to be mounted already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   */
  public void mount(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    MasterContext.getMasterSource().incMountOps(1);
    synchronized (mInodeTree) {
      mountInternal(tachyonPath, ufsPath);
      boolean loadMetadataSuceeded = false;
      try {
        // This will create the directory at tachyonPath
        loadMetadataDirectory(tachyonPath, false);
        loadMetadataSuceeded = true;
      } finally {
        if (!loadMetadataSuceeded) {
          unmountInternal(tachyonPath);
        }
        // Exception will be propagated from loadMetadataDirectory
      }
      AddMountPointEntry addMountPoint =
          AddMountPointEntry.newBuilder().setTachyonPath(tachyonPath.toString())
              .setUfsPath(ufsPath.toString()).build();
      writeJournalEntry(JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build());
      flushJournal();
      MasterContext.getMasterSource().incPathsMounted(1);
    }
  }

  void mountFromEntry(AddMountPointEntry entry)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    TachyonURI tachyonURI = new TachyonURI(entry.getTachyonPath());
    TachyonURI ufsURI = new TachyonURI(entry.getUfsPath());
    mountInternal(tachyonURI, ufsURI);
  }

  void mountInternal(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    // Check that the ufsPath exists and is a directory
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath.toString(), MasterContext.getConf());
    if (!ufs.exists(ufsPath.getPath())) {
      throw new IOException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
    }
    if (ufs.isFile(ufsPath.getPath())) {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_DIRECTORY.getMessage(ufsPath.getPath()));
    }
    // Check that the tachyonPath we're creating doesn't shadow a path in the default UFS
    String defaultUfsPath = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem defaultUfs = UnderFileSystem.get(defaultUfsPath, MasterContext.getConf());
    if (defaultUfs.exists(PathUtils.concatPath(defaultUfsPath, tachyonPath.getPath()))) {
      throw new IOException(
          ExceptionMessage.MOUNT_PATH_SHADOWS_DEFAULT_UFS.getMessage(tachyonPath));
    }
    // This should check that we are not mounting a prefix of an existing mount, and that no
    // existing mount is a prefix of this mount.
    mMountTable.add(tachyonPath, ufsPath);
  }

  /**
   * Unmounts a UFS path previously mounted path onto a Tachyon path.
   *
   * @param tachyonPath the Tachyon path path to unmount, must be a mount point
   * @return true if the UFS path was successfully unmounted, false otherwise
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   */
  public boolean unmount(TachyonURI tachyonPath)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    MasterContext.getMasterSource().incUnmountOps(1);
    synchronized (mInodeTree) {
      if (unmountInternal(tachyonPath)) {
        Inode inode = mInodeTree.getInodeByPath(tachyonPath);
        // Use the internal delete API, setting {@code replayed} to false to prevent the delete
        // operations from being persisted in the UFS.
        long fileId = inode.getId();
        long opTimeMs = System.currentTimeMillis();
        deleteFileRecursiveInternal(fileId, true /* replayed */, opTimeMs);
        DeleteFileEntry deleteFile = DeleteFileEntry.newBuilder()
            .setId(fileId)
            .setRecursive(true)
            .setOpTimeMs(opTimeMs)
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build());
        DeleteMountPointEntry deleteMountPoint = DeleteMountPointEntry.newBuilder()
            .setTachyonPath(tachyonPath.toString())
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setDeleteMountPoint(deleteMountPoint).build());
        flushJournal();
        MasterContext.getMasterSource().incPathsUnmounted(1);
        return true;
      }
    }
    return false;
  }

  void unmountFromEntry(DeleteMountPointEntry entry) throws InvalidPathException {
    TachyonURI tachyonURI = new TachyonURI(entry.getTachyonPath());
    if (!unmountInternal(tachyonURI)) {
      LOG.error("Failed to unmount {}", tachyonURI);
    }
  }

  boolean unmountInternal(TachyonURI tachyonPath) throws InvalidPathException {
    return mMountTable.delete(tachyonPath);
  }

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public void resetFile(long fileId) throws FileDoesNotExistException, InvalidPathException {
    // TODO(yupeng) check the file is not persisted
    synchronized (mInodeTree) {
      // free the file first
      free(getPath(fileId), false);
      InodeFile inodeFile = (InodeFile) mInodeTree.getInodeById(fileId);
      inodeFile.reset();
    }
  }

  /**
   * Sets the file state.
   *
   * @param path the path to set state
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws FileDoesNotExistException if the file does not exist
   */
  public void setState(TachyonURI path, SetAttributeOptions options) throws
      FileDoesNotExistException {
    MasterContext.getMasterSource().incSetStateOps(1);
    synchronized (mInodeTree) {
      long fileId = getFileId(path);
      long opTimeMs = System.currentTimeMillis();
      setStateInternal(fileId, opTimeMs, options);
      SetStateEntry.Builder setState =
          SetStateEntry.newBuilder().setId(fileId).setOpTimeMs(opTimeMs);
      if (options.hasPinned()) {
        setState.setPinned(options.getPinned());
      }
      if (options.hasTTL()) {
        setState.setTtl(options.getTTL());
      }
      if (options.hasPersisted()) {
        setState.setPersisted(options.getPersisted());
      }
      writeJournalEntry(JournalEntry.newBuilder().setSetState(setState).build());
      flushJournal();
    }
  }

  /**
   * Schedules a file for async persistence.
   *
   * @param fileId the id of the file for persistence
   * @return the id of the worker that persistence is scheduled on
   * @throws FileDoesNotExistException when the file does not exist
   */
  public long scheduleAsyncPersistence(long fileId) throws FileDoesNotExistException {
    // find the worker
    long workerId = getWorkerStoringFile(fileId);

    if (workerId == IdUtils.INVALID_WORKER_ID) {
      LOG.warn("No worker found to schedule async persistence for file {}", fileId);
      // no worker found, do nothing
      return workerId;
    }

    // update the state
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      inode.setPersistenceState(PersistenceState.IN_PROGRESS);
    }

    synchronized (mWorkerToAsyncPersistFiles) {
      if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
        mWorkerToAsyncPersistFiles.put(workerId, Sets.<Long>newHashSet());
      }
      mWorkerToAsyncPersistFiles.get(workerId).add(fileId);
    }

    // TODO(yupeng) TACHYON-1456 add fault tolerance and flush journal

    return workerId;
  }

  /**
   * Gets a worker where the given file is stored.
   *
   * @param fileId the file id, -1 if no worker can be found
   * @return the storing worker
   * @throws FileDoesNotExistException when the file does not exist on any worker
   */
  private long getWorkerStoringFile(long fileId) throws FileDoesNotExistException {
    Map<Long, Integer> workerBlockCounts = Maps.newHashMap();
    List<FileBlockInfo> blockInfoList;
    try {
      blockInfoList = getFileBlockInfoList(getPath(fileId));

      for (FileBlockInfo fileBlockInfo : blockInfoList) {
        for (BlockLocation blockLocation : fileBlockInfo.blockInfo.locations) {
          if (workerBlockCounts.containsKey(blockLocation.workerId)) {
            workerBlockCounts.put(blockLocation.workerId,
                workerBlockCounts.get(blockLocation.workerId) + 1);
          } else {
            workerBlockCounts.put(blockLocation.workerId, 1);
          }

          // TODO(yupeng) remove the requirement that all the blocks of a file must be stored on the
          // same worker, for now it returns the first worker that has all the blocks
          if (workerBlockCounts.get(blockLocation.workerId) == blockInfoList.size()) {
            return blockLocation.workerId;
          }
        }
      }
    } catch (FileDoesNotExistException e) {
      LOG.error("The file {} to persist does not exist", fileId);
      return IdUtils.INVALID_WORKER_ID;
    } catch (InvalidPathException e) {
      LOG.error("The file {} to persist does not exist", fileId);
      return IdUtils.INVALID_WORKER_ID;
    }

    if (workerBlockCounts.size() == 0) {
      LOG.error("The file " + fileId + " does not exist on any worker");
      return IdUtils.INVALID_WORKER_ID;
    }

    LOG.error("Not all the blocks of file {} stored on the same worker", fileId);
    return IdUtils.INVALID_WORKER_ID;
  }

  /**
   * Polls the files to send to the given worker for persistence. It also removes files from the
   * worker entry in {@link #mWorkerToAsyncPersistFiles}.
   *
   * @param workerId the worker id
   * @return the list of files
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  private List<PersistFile> pollFilesToCheckpoint(long workerId)
      throws FileDoesNotExistException, InvalidPathException {
    List<PersistFile> filesToPersist = Lists.newArrayList();
    List<Long> fileIdsToPersist = Lists.newArrayList();

    synchronized (mWorkerToAsyncPersistFiles) {
      synchronized (mInodeTree) {
        if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
          return filesToPersist;
        }

        Set<Long> scheduledFiles = mWorkerToAsyncPersistFiles.get(workerId);
        for (long fileId : scheduledFiles) {
          InodeFile inode = (InodeFile) mInodeTree.getInodeById(fileId);
          if (inode.isCompleted()) {
            fileIdsToPersist.add(fileId);
            List<Long> blockIds = Lists.newArrayList();
            for (FileBlockInfo fileBlockInfo : getFileBlockInfoList(mInodeTree.getPath(inode))) {
              blockIds.add(fileBlockInfo.blockInfo.blockId);
            }

            filesToPersist.add(new PersistFile(fileId, blockIds));
            // update the inode file persisence state
            inode.setPersistenceState(PersistenceState.IN_PROGRESS);
          }
        }

      }
    }
    mWorkerToAsyncPersistFiles.get(workerId).removeAll(fileIdsToPersist);

    // write to journal
    PersistFilesRequestEntry persistFilesRequest =
        PersistFilesRequestEntry.newBuilder().addAllFileIds(fileIdsToPersist).build();
    writeJournalEntry(
        JournalEntry.newBuilder().setPersistFilesRequest(persistFilesRequest).build());
    flushJournal();
    return filesToPersist;
  }

  /**
   * Updates a list of files as being persisted.
   *
   * @param fileIds the id of the files
   * @throws FileDoesNotExistException when a file does not exist
   */
  private void setPersistingState(List<Long> fileIds) throws FileDoesNotExistException {
    for (long fileId : fileIds) {
      InodeFile inode = (InodeFile) mInodeTree.getInodeById(fileId);
      inode.setPersistenceState(PersistenceState.IN_PROGRESS);
    }
  }

  /**
   * Instructs a worker to persist the files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   */
  public synchronized FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException {
    for (long fileId : persistedFiles) {
      setState(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
    }

    // get the files for the given worker to checkpoint
    List<PersistFile> filesToCheckpoint = pollFilesToCheckpoint(workerId);
    if (!filesToCheckpoint.isEmpty()) {
      LOG.debug("Sent files {} to worker {} to persist", filesToCheckpoint, workerId);
    }
    FileSystemCommandOptions options = new FileSystemCommandOptions();
    options.setPersistOptions(new PersistCommandOptions(filesToCheckpoint));
    return new FileSystemCommand(CommandType.Persist, options);
  }

  private void setStateInternal(long fileId, long opTimeMs, SetAttributeOptions options)
      throws FileDoesNotExistException {
    Inode inode = mInodeTree.getInodeById(fileId);
    if (options.hasPinned()) {
      mInodeTree.setPinned(inode, options.getPinned(), opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    if (options.hasTTL()) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.TTL_ONLY_FOR_FILE);
      long ttl = options.getTTL();
      InodeFile file = (InodeFile) inode;
      if (file.getTTL() != ttl) {
        mTTLBuckets.remove(file);
        file.setTTL(ttl);
        mTTLBuckets.insert(file);
        file.setLastModificationTimeMs(opTimeMs);
      }
    }
    if (options.hasPersisted()) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(((InodeFile) inode).isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      InodeFile file = (InodeFile) inode;
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions.checkArgument(options.getPersisted(),
          PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!file.isPersisted()) {
        file.setPersistenceState(PersistenceState.PERSISTED);
        propagatePersisted(file, false);
        file.setLastModificationTimeMs(opTimeMs);
        MasterContext.getMasterSource().incFilesPersisted(1);
      }
    }
  }

  // TODO(calvin): Rename SetStateEntry to SetAttributeEntry, do not rely on client side options
  private void setStateFromEntry(SetStateEntry entry) throws FileDoesNotExistException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    if (entry.hasPinned()) {
      options.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      options.setTTL(entry.getTtl());
    }
    if (entry.hasPersisted()) {
      options.setPersisted(entry.getPersisted());
    }
    setStateInternal(entry.getId(), entry.getOpTimeMs(), options);
  }

  /**
   * This class represents the executor for periodic inode TTL check.
   */
  private final class MasterInodeTTLCheckExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      synchronized (mInodeTree) {
        Set<TTLBucket> expiredBuckets = mTTLBuckets.getExpiredBuckets(System.currentTimeMillis());
        for (TTLBucket bucket : expiredBuckets) {
          for (InodeFile file : bucket.getFiles()) {
            if (!file.isDeleted()) {
              // file.isPinned() is deliberately not checked because ttl will have effect no matter
              // whether the file is pinned.
              try {
                deleteFile(mInodeTree.getPath(file), false);
              } catch (Exception e) {
                LOG.error("Exception trying to clean up {} for ttl check: {}", file.toString(),
                    e.toString());
              }
            }
          }
        }

        mTTLBuckets.removeBuckets(expiredBuckets);
      }
    }
  }
}
