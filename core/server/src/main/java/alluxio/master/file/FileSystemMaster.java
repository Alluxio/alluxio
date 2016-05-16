/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.collections.PrefixList;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.JournalProtoUtils;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.DeleteMountPointEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.ReinitializeFileEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.StringPairEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemCommandOptions;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.PersistCommandOptions;
import alluxio.thrift.PersistFile;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The master that handles all file system metadata management.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** This manages the file system inode structure. This must be journaled. */
  @GuardedBy("itself")
  private final InodeTree mInodeTree;

  /** This manages the file system mount points. */
  @GuardedBy("mInodeTree")
  private final MountTable mMountTable;

  /** Map from worker to the files to persist on that worker. Used by async persistence service. */
  @GuardedBy("mInodeTree")
  private final Map<Long, Set<Long>> mWorkerToAsyncPersistFiles;

  /** This maintains inodes with ttl set, for the for the ttl checker service to use. */
  @GuardedBy("mInodeTree")
  private final TtlBucketList mTtlBuckets = new TtlBucketList();

  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /** This checks user permissions on different operations. */
  @GuardedBy("mInodeTree")
  private final PermissionChecker mPermissionChecker;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /** The handler for async persistence. */
  private final AsyncPersistHandler mAsyncPersistHandler;

  /**
   * The service that checks for inode files with ttl set. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mTtlCheckerService;

  /**
   * The service that detects lost files. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostFilesDetectionService;

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
    Configuration conf = MasterContext.getConf();
    mWhitelist = new PrefixList(conf.getList(Constants.MASTER_WHITELIST, ","));

    mWorkerToAsyncPersistFiles = new HashMap<>();
    mAsyncPersistHandler =
        AsyncPersistHandler.Factory.create(MasterContext.getConf(), new FileSystemMasterView(this));
    mPermissionChecker = new PermissionChecker(mInodeTree);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(
        Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientService.Processor<>(
            new FileSystemMasterClientServiceHandler(this)));
    services.put(
        Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<>(
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
      try {
        mInodeTree.addInodeFromJournal(entry);
      } catch (AccessControlException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof InodeLastModificationTimeEntry) {
      InodeLastModificationTimeEntry modTimeEntry = (InodeLastModificationTimeEntry) innerEntry;
      try {
        Inode<?> inode = mInodeTree.getInodeById(modTimeEntry.getId());
        inode.setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof PersistDirectoryEntry) {
      PersistDirectoryEntry typedEntry = (PersistDirectoryEntry) innerEntry;
      try {
        Inode<?> inode = mInodeTree.getInodeById(typedEntry.getId());
        inode.setPersistenceState(PersistenceState.PERSISTED);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof CompleteFileEntry) {
      try {
        completeFileFromEntry((CompleteFileEntry) innerEntry);
      } catch (InvalidPathException | InvalidFileSizeException | FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof SetAttributeEntry) {
      try {
        setAttributeFromEntry((SetAttributeEntry) innerEntry);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof DeleteFileEntry) {
      deleteFromEntry((DeleteFileEntry) innerEntry);
    } else if (innerEntry instanceof RenameEntry) {
      renameFromEntry((RenameEntry) innerEntry);
    } else if (innerEntry instanceof InodeDirectoryIdGeneratorEntry) {
      mDirectoryIdGenerator.initFromJournalEntry((InodeDirectoryIdGeneratorEntry) innerEntry);
    } else if (innerEntry instanceof ReinitializeFileEntry) {
      resetBlockFileFromEntry((ReinitializeFileEntry) innerEntry);
    } else if (innerEntry instanceof AddMountPointEntry) {
      try {
        mountFromEntry((AddMountPointEntry) innerEntry);
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof DeleteMountPointEntry) {
      try {
        unmountFromEntry((DeleteMountPointEntry) innerEntry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof AsyncPersistRequestEntry) {
      try {
        long fileId = ((AsyncPersistRequestEntry) innerEntry).getFileId();
        scheduleAsyncPersistenceInternal(fileId);
        // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
        mAsyncPersistHandler.scheduleAsyncPersistence(getPath(fileId));
      } catch (AlluxioException e) {
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
      mInodeTree.initializeRoot(PermissionStatus.defaults()
          .applyDirectoryUMask(MasterContext.getConf())
          .setUserFromLoginModule(MasterContext.getConf()));
      String defaultUFS = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
      try {
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(defaultUFS),
            MountOptions.defaults());
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
    }
    // Call super.start after mInodeTree is initialized because mInodeTree is needed to write
    // a journal entry during super.start. Call super.start before calling
    // getExecutorService() because the super.start initializes the executor service.
    super.start(isLeader);
    if (isLeader) {
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTtlCheckExecutor(),
              MasterContext.getConf().getInt(Constants.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_FILES_DETECTION, new LostFilesDetectionHeartbeatExecutor(),
          MasterContext.getConf().getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS)));
    }
  }

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   * <p>
   * This operation requires users to have {@link FileSystemAction#READ} permission of the path.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  public long getFileId(AlluxioURI path) throws AccessControlException, FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode<?> inode;
      try {
        mPermissionChecker.checkPermission(FileSystemAction.READ, path);
        if (!mInodeTree.inodePathExists(path)) {
          try {
            return loadMetadata(path, true);
          } catch (Exception e) {
            return IdUtils.INVALID_FILE_ID;
          }
        }
        inode = mInodeTree.getInodeByPath(path);
      } catch (InvalidPathException e) {
        return IdUtils.INVALID_FILE_ID;
      }
      return inode.getId();
    }
  }

  /**
   * Returns the {@link FileInfo} for a given file id. This method is not user-facing but supposed
   * to be called by other internal servers (e.g., block workers and web UI servers).
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Currently used by Lineage Master and WebUI
  // TODO(binfan): Add permission checking for internal APIs
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have {@link FileSystemAction#READ} permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  public FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, path);
      // getFileInfo should load from ufs if the file does not exist
      getFileId(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * @param inode the inode to get the {@linke FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   */
  @GuardedBy("mInodeTree")
  private FileInfo getFileInfoInternal(Inode<?> inode) throws FileDoesNotExistException {
    FileInfo fileInfo = inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    if (inode instanceof InodeFile) {
      InodeFile inodeFile = (InodeFile) inode;
      try {
        fileInfo.setFileBlockInfos(getFileBlockInfoListInternal(inodeFile));
      } catch (InvalidPathException e) {
        throw new FileDoesNotExistException(e.getMessage(), e);
      }
    }
    AlluxioURI path = mInodeTree.getPath(inode);
    MountTable.Resolution resolution;
    try {
      resolution = mMountTable.resolve(path);
    } catch (InvalidPathException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    }
    AlluxioURI resolvedUri = resolution.getUri();
    // Only set the UFS path if the path is nested under a mount point.
    if (!path.equals(resolvedUri)) {
      fileInfo.setUfsPath(resolvedUri.toString());
    }
    MasterContext.getMasterSource().incFileInfosGot(1);
    return fileInfo;
  }

  /**
   * @param fileId the file id
   * @return the persistence state for the given file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Internal facing, currently used by Lineage master
  // TODO(binfan): Add permission checking for internal APIs
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
      return inode.getPersistenceState();
    }
  }

  /**
   * Returns a list {@link FileInfo} for a given path. If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * <p>
   * This operation requires users to have
   * {@link FileSystemAction#READ} permission on the path, and also
   * {@link FileSystemAction#EXECUTE} permission on the path if it is a directory.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @return the list of {@link FileInfo}s
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  public List<FileInfo> getFileInfoList(AlluxioURI path)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, path);
      // getFileInfoList should load from ufs if the file does not exist
      getFileId(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);

      List<FileInfo> ret = new ArrayList<>();
      if (inode.isDirectory()) {
        mPermissionChecker.checkPermission(FileSystemAction.EXECUTE, path);
        for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
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
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the path.
   *
   * @param path the file path to complete
   * @param options the method options
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws AccessControlException if permission checking fails
   */
  public void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException {
    MasterContext.getMasterSource().incCompleteFileOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkPermission(FileSystemAction.WRITE, path);
      // Even readonly mount points should be able to complete a file, for UFS reads in CACHE mode.
      long opTimeMs = System.currentTimeMillis();
      Inode<?> inode = mInodeTree.getInodeByPath(path);
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

      // Iterate over all file blocks committed to Alluxio, computing the length and verify that all
      // the blocks (except the last one) is the same size as the file block size.
      long inMemoryLength = 0;
      long fileBlockSize = fileInode.getBlockSizeBytes();
      for (int i = 0; i < blockInfoList.size(); i++) {
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

  /**
   * @param blockIds the block ids to use
   * @param fileId the file id to use
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  @GuardedBy("mInodeTree")
  void completeFileInternal(List<Long> blockIds, long fileId, long length, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException {
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

  /**
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  @GuardedBy("mInodeTree")
  private void completeFileFromEntry(CompleteFileEntry entry)
      throws InvalidPathException, InvalidFileSizeException, FileAlreadyCompletedException {
    try {
      completeFileInternal(entry.getBlockIdsList(), entry.getId(), entry.getLength(),
          entry.getOpTimeMs());
    } catch (FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * This operation requires {@link FileSystemAction#WRITE} permission on the parent of this path.
   *
   * @param path the file to create
   * @param options method options
   * @return the file id of the create file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information in encountered
   * @throws IOException if the creation fails
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
          BlockInfoException, IOException, FileDoesNotExistException {
    MasterContext.getMasterSource().incCreateFileOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, path);
      if (!options.isMetadataLoad()) {
        mMountTable.checkUnderWritableMountPoint(path);
      }
      InodeTree.CreatePathResult createResult = createFileInternal(path, options);
      List<Inode<?>> created = createResult.getCreated();

      writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
      journalCreatePathResult(createResult);
      flushJournal();
      return created.get(created.size() - 1).getId();
    }
  }

  /**
   * @param path the path to be created
   * @param options method options
   * @return {@link InodeTree.CreatePathResult} with the path creation result
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if invalid block information is encountered
   * @throws IOException if an I/O error occurs
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  @GuardedBy("mInodeTree")
  InodeTree.CreatePathResult createFileInternal(AlluxioURI path, CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      FileDoesNotExistException {
    InodeTree.CreatePathResult createResult = mInodeTree.createPath(path, options);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode<?>> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(path.toString())) {
      inode.setCacheable(true);
    }

    mTtlBuckets.insert(inode);

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
   * @throws FileDoesNotExistException if the path does not exist
   */
  // Used by lineage master
  public long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl)
      throws InvalidPathException, FileDoesNotExistException {
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

  /**
   * @param entry the entry to use
   */
  @GuardedBy("mInodeTree")
  private void resetBlockFileFromEntry(ReinitializeFileEntry entry) {
    try {
      mInodeTree.reinitializeFile(new AlluxioURI(entry.getPath()), entry.getBlockSizeBytes(),
          entry.getTtl());
    } catch (InvalidPathException | FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a new block id for the next block of a given file to write to.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the path as
   * this API is called when creating a new block for a file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   * @throws AccessControlException if permission checking fails
   */
  public long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incGetNewBlockOps(1);
    InodeFile inode;
    synchronized (mInodeTree) {
      mPermissionChecker.checkPermission(FileSystemAction.WRITE, path);
      inode = mInodeTree.getInodeFileByPath(path);
    }
    MasterContext.getMasterSource().incNewBlocksGot(1);
    return inode.getNewBlockId();
  }

  /**
   * @return the number of files and directories
   */
  public int getNumberOfPaths() {
    synchronized (mInodeTree) {
      return mInodeTree.getSize();
    }
  }

  /**
   * @return the number of pinned files and directories
   */
  public int getNumberOfPinnedFiles() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinnedSize();
    }
  }

  /**
   * Deletes a given path.
   * <p>
   * This operation requires user to have {@link FileSystemAction#WRITE}
   * permission on the parent of the path.
   *
   * @param path the path to delete
   * @param recursive if true, will delete all its children
   * @return true if the file was deleted, false otherwise
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public boolean delete(AlluxioURI path, boolean recursive)
      throws IOException, FileDoesNotExistException, DirectoryNotEmptyException,
          InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incDeletePathOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, path);
      mMountTable.checkUnderWritableMountPoint(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);
      long fileId = inode.getId();
      long opTimeMs = System.currentTimeMillis();
      boolean ret = deleteInternal(fileId, recursive, false, opTimeMs);
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

  /**
   * @param entry the entry to use
   */
  @GuardedBy("mInodeTree")
  private void deleteFromEntry(DeleteFileEntry entry) {
    MasterContext.getMasterSource().incDeletePathOps(1);
    try {
      deleteInternal(entry.getId(), entry.getRecursive(), true, entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for avoiding {@link DirectoryNotEmptyException} when calling
   * {@link #deleteInternal(long, boolean, boolean, long)}.
   *
   * @param fileId the file id
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return if the file is successfully deleted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   */
  @GuardedBy("mInodeTree")
  private boolean deleteRecursiveInternal(long fileId, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, IOException {
    try {
      return deleteInternal(fileId, true, replayed, opTimeMs);
    } catch (DirectoryNotEmptyException e) {
      throw new IllegalStateException(
          "deleteInternal should never throw DirectoryNotEmptyException when recursive is true",
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
  @GuardedBy("mInodeTree")
  boolean deleteInternal(long fileId, boolean recursive, boolean replayed,
      long opTimeMs) throws FileDoesNotExistException, IOException, DirectoryNotEmptyException {
    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Alluxio and UFS.
    if (!mInodeTree.inodeIdExists(fileId)) {
      return true;
    }
    Inode<?> inode = mInodeTree.getInodeById(fileId);
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

    List<Inode<?>> delInodes = new ArrayList<Inode<?>>();
    delInodes.add(inode);
    if (inode.isDirectory()) {
      delInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
    }

    // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
    // file, we deal with the checkpoints and blocks as well.
    for (int i = delInodes.size() - 1; i >= 0; i--) {
      Inode<?> delInode = delInodes.get(i);

      // TODO(jiri): What should the Alluxio behavior be when a UFS delete operation fails?
      // Currently, it will result in an inconsistency between Alluxio and UFS.
      if (!replayed && delInode.isPersisted()) {
        try {
          AlluxioURI alluxioUriToDel = mInodeTree.getPath(delInode);
          // If this is a mount point, we have deleted all the children and can unmount it
          // TODO(calvin): Add tests (ALLUXIO-1831)
          if (mMountTable.isMountPoint(alluxioUriToDel)) {
            unmountInternal(alluxioUriToDel);
          } else {
            // Delete the file in the under file system.
            MountTable.Resolution resolution = mMountTable.resolve(alluxioUriToDel);
            String ufsUri = resolution.getUri().toString();
            UnderFileSystem ufs = resolution.getUfs();
            if (!ufs.exists(ufsUri)) {
              LOG.warn("File does not exist the underfs: {}", ufsUri);
            } else if (!ufs.delete(ufsUri, true)) {
              LOG.error("Failed to delete {}", ufsUri);
              return false;
            }
          }
        } catch (InvalidPathException e) {
          LOG.warn(e.getMessage());
        }
      }

      if (delInode.isFile()) {
        // Remove corresponding blocks from workers and delete metadata in master.
        mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds(), true /* delete */);
      }

      mInodeTree.deleteInode(delInode, opTimeMs);
    }
    MasterContext.getMasterSource().incPathsDeleted(delInodes.size());
    return true;
  }

  /**
   * Gets the {@link FileBlockInfo} for all blocks of a file. If path is a directory, an exception
   * is thrown.
   * <p>
   * This operation requires the client user to have {@link FileSystemAction#READ} permission on the
   * the path.
   *
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given path
   * @throws FileDoesNotExistException if the file does not exist or path is a directory
   * @throws InvalidPathException if the path of the given file is invalid
   * @throws AccessControlException if permission checking fails
   */
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incGetFileBlockInfoOps(1);
    synchronized (mInodeTree) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, path);
      InodeFile inode = mInodeTree.getInodeFileByPath(path);
      List<FileBlockInfo> ret = getFileBlockInfoListInternal(inode);
      MasterContext.getMasterSource().incFileBlockInfosGot(ret.size());
      return ret;
    }
  }

  /**
   * @param file the inode to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given inode
   * @throws InvalidPathException if the path of the given file is invalid
   */
  @GuardedBy("mInodeTree")
  private List<FileBlockInfo> getFileBlockInfoListInternal(InodeFile file)
    throws InvalidPathException {
    List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(file.getBlockIds());

    List<FileBlockInfo> ret = new ArrayList<>();
    for (BlockInfo blockInfo : blockInfoList) {
      ret.add(generateFileBlockInfo(file, blockInfo));
    }
    return ret;
  }

  /**
   * Generates a {@link FileBlockInfo} object from internal metadata. This adds file information to
   * the block, such as the file offset, and additional UFS locations for the block.
   *
   * @param file the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   * @throws InvalidPathException if the mount table is not able to resolve the file
   */
  @GuardedBy("mInodeTree")
  private FileBlockInfo generateFileBlockInfo(InodeFile file, BlockInfo blockInfo)
      throws InvalidPathException {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setUfsLocations(new ArrayList<String>());

    // The sequence number part of the block id is the block index.
    long offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.getBlockId());
    fileBlockInfo.setOffset(offset);

    if (fileBlockInfo.getBlockInfo().getLocations().isEmpty() && file.isPersisted()) {
      // No alluxio locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      MountTable.Resolution resolution = mMountTable.resolve(mInodeTree.getPath(file));
      String ufsUri = resolution.getUri().toString();
      UnderFileSystem ufs = resolution.getUfs();
      List<String> locs;
      try {
        locs = ufs.getFileLocations(ufsUri, fileBlockInfo.getOffset());
      } catch (IOException e) {
        return fileBlockInfo;
      }
      if (locs != null) {
        for (String loc : locs) {
          fileBlockInfo.getUfsLocations().add(loc);
        }
      }
    }
    return fileBlockInfo;
  }
