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
import alluxio.master.file.meta.InodeLockGroup;
import alluxio.master.file.meta.InodePath;
import alluxio.master.file.meta.InodePathPair;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TempInodePathWithDescendant;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.AsyncJournalWriter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
      try (InodePath inodePath = mInodeTree.lockFullInodePath(modTimeEntry.getId(),
          InodeTree.LockMode.WRITE)) {
        inodePath.getInode().setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof PersistDirectoryEntry) {
      PersistDirectoryEntry typedEntry = (PersistDirectoryEntry) innerEntry;
      try (InodePath inodePath = mInodeTree.lockFullInodePath(typedEntry.getId(),
          InodeTree.LockMode.WRITE)) {
        inodePath.getInode().setPersistenceState(PersistenceState.PERSISTED);
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
      } catch (InvalidPathException | FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof AsyncPersistRequestEntry) {
      try {
        long fileId = ((AsyncPersistRequestEntry) innerEntry).getFileId();
        try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          scheduleAsyncPersistenceInternal(inodePath);
        }
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, inodePath);
      flushCounter = loadMetadataIfNotExistAndJournal(inodePath);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return inodePath.getInode().getId();
    } catch (InvalidPathException | FileDoesNotExistException e) {
      return IdUtils.INVALID_FILE_ID;
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
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
    try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return getFileInfoInternal(inodePath);
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, inodePath);
      flushCounter = loadMetadataIfNotExistAndJournal(inodePath);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return getFileInfoInternal(inodePath);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * @param inodePath the {@link InodePath} to get the {@linke FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   */
  @GuardedBy("mInodeTree")
  private FileInfo getFileInfoInternal(InodePath inodePath) throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    AlluxioURI path = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(path.toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    if (inode instanceof InodeFile) {
      try {
        fileInfo.setFileBlockInfos(getFileBlockInfoListInternal(inodePath));
      } catch (InvalidPathException e) {
        throw new FileDoesNotExistException(e.getMessage(), e);
      }
    }
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
    try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return inodePath.getInode().getPersistenceState();
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, inodePath);
      flushCounter = loadMetadataIfNotExistAndJournal(inodePath);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      Inode<?> inode = inodePath.getInode();

      List<FileInfo> ret = new ArrayList<>();
      if (inode.isDirectory()) {
        TempInodePathWithDescendant tempInodePath = new TempInodePathWithDescendant(inodePath);
        mPermissionChecker.checkPermission(FileSystemAction.EXECUTE, inodePath);
        for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
          child.lockRead();
          try {
            // the path to child for getPath should already be locked.
            tempInodePath.setDescendant(child, mInodeTree.getPath(child));
            ret.add(getFileInfoInternal(tempInodePath));
          } finally {
            child.unlockRead();
          }
        }
      } else {
        ret.add(getFileInfoInternal(inodePath));
      }
      MasterContext.getMasterSource().incFileInfosGot(ret.size());
      return ret;
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkPermission(FileSystemAction.WRITE, inodePath);
      // Even readonly mount points should be able to complete a file, for UFS reads in CACHE mode.
      flushCounter = completeFileAndJournal(inodePath, options);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link InodePath} to complete
   * @param options the method options
   * @return the flush counter for journaling
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if an invalid file size is encountered
   */
  long completeFileAndJournal(InodePath inodePath, CompleteFileOptions options)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException {
    long opTimeMs = System.currentTimeMillis();
    Inode<?> inode = inodePath.getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_MUST_BE_FILE.getMessage(inodePath.getUri()));
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

    completeFileInternal(fileInode.getBlockIds(), inodePath, length, opTimeMs);
    CompleteFileEntry completeFileEntry = CompleteFileEntry.newBuilder()
        .addAllBlockIds(fileInode.getBlockIds())
        .setId(inode.getId())
        .setLength(length)
        .setOpTimeMs(opTimeMs)
        .build();
    return appendJournalEntry(JournalEntry.newBuilder().setCompleteFile(completeFileEntry).build());
  }

  /**
   * @param blockIds the block ids to use
   * @param inodePath the {@link InodePath} to complete
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  @GuardedBy("mInodeTree")
  void completeFileInternal(List<Long> blockIds, InodePath inodePath, long length, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException {
    InodeFile inode = inodePath.getInodeFile();
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
    try (InodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      completeFileInternal(entry.getBlockIdsList(), inodePath, entry.getLength(),
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      flushCounter = createFileAndJournal(inodePath, options);
      return inodePath.getInode().getId();
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the file to create
   * @param options method options
   * @return the file id of the create file
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information in encountered
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if the creation fails
   */
  long createFileAndJournal(InodePath inodePath, CreateFileOptions options)
      throws FileAlreadyExistsException, BlockInfoException, FileDoesNotExistException,
      InvalidPathException, IOException {
    InodeTree.CreatePathResult createResult = createFileInternal(inodePath, options);

    long counter = appendJournalEntry(mDirectoryIdGenerator.toJournalEntry());
    counter = AsyncJournalWriter.getFlushCounter(counter, journalCreatePathResult(createResult));
    return counter;
  }

  /**
   * @param inodePath the path to be created
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
  InodeTree.CreatePathResult createFileInternal(InodePath inodePath, CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      FileDoesNotExistException {
    InodeTree.CreatePathResult createResult = mInodeTree.createPath(inodePath, options);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode<?>> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(inodePath.getUri().toString())) {
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      long id = mInodeTree.reinitializeFile(inodePath, blockSizeBytes, ttl);
      ReinitializeFileEntry reinitializeFile = ReinitializeFileEntry.newBuilder()
          .setPath(path.getPath())
          .setBlockSizeBytes(blockSizeBytes)
          .setTtl(ttl)
          .build();
      flushCounter = appendJournalEntry(
          JournalEntry.newBuilder().setReinitializeFile(reinitializeFile).build());
      return id;
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * @param entry the entry to use
   */
  @GuardedBy("mInodeTree")
  private void resetBlockFileFromEntry(ReinitializeFileEntry entry) {
    try (InodePath inodePath = mInodeTree
        .lockFullInodePath(new AlluxioURI(entry.getPath()), InodeTree.LockMode.WRITE)) {
      mInodeTree.reinitializeFile(inodePath, entry.getBlockSizeBytes(), entry.getTtl());
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
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkPermission(FileSystemAction.WRITE, inodePath);
      MasterContext.getMasterSource().incNewBlocksGot(1);
      return inodePath.getInodeFile().getNewBlockId();
    }
  }

  /**
   * @return the number of files and directories
   */
  public int getNumberOfPaths() {
    return mInodeTree.getSize();
  }

  /**
   * @return the number of pinned files and directories
   */
  public int getNumberOfPinnedFiles() {
    return mInodeTree.getPinnedSize();
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
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      flushCounter = deleteAndJournal(inodePath, recursive);
      return true;
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Deletes a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the path to delete
   * @param recursive if true, will delete all its children
   * @return the flush counter for journaling
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  long deleteAndJournal(InodePath inodePath, boolean recursive)
      throws InvalidPathException, FileDoesNotExistException, IOException,
      DirectoryNotEmptyException {
    Inode<?> inode = inodePath.getInode();
    long fileId = inode.getId();
    long opTimeMs = System.currentTimeMillis();
    deleteInternal(inodePath, recursive, false, opTimeMs);
    DeleteFileEntry deleteFile = DeleteFileEntry.newBuilder()
        .setId(fileId)
        .setRecursive(recursive)
        .setOpTimeMs(opTimeMs)
        .build();
    return appendJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build());
  }

  /**
   * @param entry the entry to use
   */
  @GuardedBy("mInodeTree")
  private void deleteFromEntry(DeleteFileEntry entry) {
    MasterContext.getMasterSource().incDeletePathOps(1);
    try (InodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      deleteInternal(inodePath, entry.getRecursive(), true, entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for avoiding {@link DirectoryNotEmptyException} when calling
   * {@link #deleteInternal(InodePath, boolean, boolean, long)}.
   *
   * @param inodePath the {@link InodePath} to delete
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return if the file is successfully deleted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   */
  @GuardedBy("mInodeTree")
  private boolean deleteRecursiveInternal(InodePath inodePath, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, IOException {
    try {
      return deleteInternal(inodePath, true, replayed, opTimeMs);
    } catch (DirectoryNotEmptyException e) {
      throw new IllegalStateException(
          "deleteInternal should never throw DirectoryNotEmptyException when recursive is true",
          e);
    }
  }

  /**
   * Implements file deletion.
   *
   * @param inodePath the file {@link InodePath}
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
  boolean deleteInternal(InodePath inodePath, boolean recursive, boolean replayed,
      long opTimeMs) throws FileDoesNotExistException, IOException, DirectoryNotEmptyException {
    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Alluxio and UFS.
    if (!inodePath.fullPathExists()) {
      return true;
    }
    Inode<?> inode = inodePath.getInode();
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

    try (InodeLockGroup lockGroup = mInodeTree
        .getInodeChildrenRecursive(inodePath, InodeTree.LockMode.WRITE)) {
      delInodes.addAll(lockGroup.getInodes());

      TempInodePathWithDescendant tempInodePath = new TempInodePathWithDescendant(inodePath);
      // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (int i = delInodes.size() - 1; i >= 0; i--) {
        Inode<?> delInode = delInodes.get(i);
        // the path to delInode for getPath should already be locked.
        AlluxioURI alluxioUriToDel = mInodeTree.getPath(delInode);
        tempInodePath.setDescendant(delInode, alluxioUriToDel);

        // TODO(jiri): What should the Alluxio behavior be when a UFS delete operation fails?
        // Currently, it will result in an inconsistency between Alluxio and UFS.
        if (!replayed && delInode.isPersisted()) {
          try {
            // If this is a mount point, we have deleted all the children and can unmount it
            // TODO(calvin): Add tests (ALLUXIO-1831)
            if (mMountTable.isMountPoint(alluxioUriToDel)) {
              unmountInternal(tempInodePath);
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

        mInodeTree.deleteInode(tempInodePath, opTimeMs);
      }
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
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, inodePath);
      List<FileBlockInfo> ret = getFileBlockInfoListInternal(inodePath);
      MasterContext.getMasterSource().incFileBlockInfosGot(ret.size());
      return ret;
    }
  }

  /**
   * @param inodePath the {@link InodePath} to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given inode
   * @throws InvalidPathException if the path of the given file is invalid
   */
  @GuardedBy("mInodeTree")
  private List<FileBlockInfo> getFileBlockInfoListInternal(InodePath inodePath)
    throws InvalidPathException, FileDoesNotExistException {
    InodeFile file = inodePath.getInodeFile();
    List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(file.getBlockIds());

    List<FileBlockInfo> ret = new ArrayList<>();
    for (BlockInfo blockInfo : blockInfoList) {
      ret.add(generateFileBlockInfo(inodePath, blockInfo));
    }
    return ret;
  }

  /**
   * Generates a {@link FileBlockInfo} object from internal metadata. This adds file information to
   * the block, such as the file offset, and additional UFS locations for the block.
   *
   * @param inodePath the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   * @throws InvalidPathException if the mount table is not able to resolve the file
   */
  @GuardedBy("mInodeTree")
  private FileBlockInfo generateFileBlockInfo(InodePath inodePath, BlockInfo blockInfo)
      throws InvalidPathException, FileDoesNotExistException {
    InodeFile file = inodePath.getInodeFile();
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setUfsLocations(new ArrayList<String>());

    // The sequence number part of the block id is the block index.
    long offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.getBlockId());
    fileBlockInfo.setOffset(offset);

    if (fileBlockInfo.getBlockInfo().getLocations().isEmpty() && file.isPersisted()) {
      // No alluxio locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
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
   * @return absolute paths of all in memory files
   */
  public List<AlluxioURI> getInMemoryFiles() {
    List<AlluxioURI> ret = new ArrayList<AlluxioURI>();
    getInMemoryFilesInternal(mInodeTree.getRoot(), new AlluxioURI(AlluxioURI.SEPARATOR), ret);
    return ret;
  }

  private void getInMemoryFilesInternal(Inode<?> inode, AlluxioURI uri,
      List<AlluxioURI> inMemoryFiles) {
    inode.lockRead();
    try {
      AlluxioURI newUri = uri.join(inode.getName());
      if (inode.isFile()) {
        if (isFullyInMemory((InodeFile) inode)) {
          inMemoryFiles.add(newUri);
        }
      } else {
        // This inode is a directory.
        Set<Inode<?>> children = ((InodeDirectory) inode).getChildren();
        for (Inode<?> child : children) {
          getInMemoryFilesInternal(child, newUri, inMemoryFiles);
        }
      }
    } finally {
      inode.unlockRead();
    }
  }

  /**
   * Gets the in-memory percentage of an Inode. For a file that has all blocks in memory, it returns
   * 100; for a file that has no block in memory, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in memory percentage
   */
  private int getInMemoryPercentage(Inode<?> inode) {
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
   * Creates a directory for a given path.
   * <p>
   * This operation requires the client user to have
   * {@link FileSystemAction#WRITE} permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param options method options
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath(AlluxioURI, CreatePathOptions)} for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    LOG.debug("createDirectory {} ", path);
    MasterContext.getMasterSource().incCreateDirectoriesOps(1);
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      flushCounter = createDirectoryAndJournal(inodePath, options);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Creates a directory for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link InodePath} of the directory
   * @param options method options
   * @return the flush counter for journaling
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath(InodePath, CreatePathOptions)} for more details
   * @throws AccessControlException if permission checking fails
   * @throws IOException if a non-Alluxio related exception occurs
   */
  long createDirectoryAndJournal(InodePath inodePath, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException {
    InodeTree.CreatePathResult createResult = createDirectoryInternal(inodePath, options);
    long counter = appendJournalEntry(mDirectoryIdGenerator.toJournalEntry());
    counter = AsyncJournalWriter.getFlushCounter(counter, journalCreatePathResult(createResult));
    MasterContext.getMasterSource().incDirectoriesCreated(1);
    return counter;
  }

  /**
   * Implementation of directory creation for a given path.
   *
   * @param inodePath the path of the directory
   * @param options method options
   * @return an {@link alluxio.master.file.meta.InodeTree.CreatePathResult} representing the
   *         modified inodes and created inodes during path creation
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath(InodePath, CreatePathOptions)} for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   */
  InodeTree.CreatePathResult createDirectoryInternal(InodePath inodePath,
      CreateDirectoryOptions options) throws InvalidPathException, FileAlreadyExistsException,
      IOException, AccessControlException, FileDoesNotExistException {
    try {
      return mInodeTree.createPath(inodePath, options);
    } catch (BlockInfoException e) {
      // Since we are creating a directory, the block size is ignored, no such exception should
      // happen.
      Throwables.propagate(e);
    }
    return null;
  }

  /**
   * Journals the {@link InodeTree.CreatePathResult}. This does not flush the journal.
   * Synchronization is required outside of this method.
   *
   * @param createResult the {@link InodeTree.CreatePathResult} to journal
   * @return the flush counter for journaling
   */
  private long journalCreatePathResult(InodeTree.CreatePathResult createResult) {
    long counter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    for (Inode<?> inode : createResult.getModified()) {
      InodeLastModificationTimeEntry inodeLastModificationTime =
          InodeLastModificationTimeEntry.newBuilder()
          .setId(inode.getId())
          .setLastModificationTimeMs(inode.getLastModificationTimeMs())
          .build();
      counter = appendJournalEntry(JournalEntry.newBuilder()
          .setInodeLastModificationTime(inodeLastModificationTime).build());
    }
    for (Inode<?> inode : createResult.getCreated()) {
      counter = appendJournalEntry(inode.toJournalEntry());
    }
    for (Inode<?> inode : createResult.getPersisted()) {
      PersistDirectoryEntry persistDirectory = PersistDirectoryEntry.newBuilder()
          .setId(inode.getId())
          .build();
      counter = appendJournalEntry(
          JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
    }
    return counter;
  }

  /**
   * Renames a file to a destination.
   * <p>
   * This operation requires users to have
   * {@link FileSystemAction#WRITE} permission on the parent of the src path, and
   * {@link FileSystemAction#WRITE} permission on the parent of the dst path.
   *
   * @param srcPath the source path to rename
   * @param dstPath the destination path to rename the file to
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileAlreadyExistsException if the file already exists
   */
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath) throws FileAlreadyExistsException,
      FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    MasterContext.getMasterSource().incRenamePathOps(1);
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePathPair inodePathPair = mInodeTree
        .lockInodePathPair(srcPath, InodeTree.LockMode.WRITE_PARENT, dstPath,
            InodeTree.LockMode.WRITE)) {
      InodePath srcInodePath = inodePathPair.getFirst();
      InodePath dstInodePath = inodePathPair.getSecond();
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, srcInodePath);
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, dstInodePath);
      mMountTable.checkUnderWritableMountPoint(srcPath);
      mMountTable.checkUnderWritableMountPoint(dstPath);
      flushCounter = renameAndJournal(srcInodePath, dstInodePath);
      LOG.debug("Renamed {} to {}", srcPath, dstPath);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Renames a file to a destination.
   * <p>
   * Writes to the journal.
   *
   * @param srcInodePath the source path to rename
   * @param dstInodePath the destination path to rename the file to
   * @return the flush counter for journaling
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws IOException if an I/O error occurs
   */
  long renameAndJournal(InodePath srcInodePath, InodePath dstInodePath)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException {
    if (!srcInodePath.fullPathExists()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcInodePath.getUri()));
    }

    Inode<?> srcInode = srcInodePath.getInode();
    // Renaming path to itself is a no-op.
    if (srcInodePath.getUri().equals(dstInodePath.getUri())) {
      return AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    }
    // Renaming the root is not allowed.
    if (srcInodePath.getUri().isRoot()) {
      throw new InvalidPathException(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage());
    }
    if (dstInodePath.getUri().isRoot()) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage());
    }
    // Renaming across mount points is not allowed.
    String srcMount = mMountTable.getMountPoint(srcInodePath.getUri());
    String dstMount = mMountTable.getMountPoint(dstInodePath.getUri());
    if ((srcMount == null && dstMount != null) || (srcMount != null && dstMount == null)
        || (srcMount != null && dstMount != null && !srcMount.equals(dstMount))) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_ACROSS_MOUNTS.getMessage(
          srcInodePath.getUri(), dstInodePath.getUri()));
    }
    // Renaming onto a mount point is not allowed.
    if (mMountTable.isMountPoint(dstInodePath.getUri())) {
      throw new InvalidPathException(
          ExceptionMessage.RENAME_CANNOT_BE_ONTO_MOUNT_POINT.getMessage(dstInodePath.getUri()));
    }
    // Renaming a path to one of its subpaths is not allowed. Check for that, by making sure
    // srcComponents isn't a prefix of dstComponents.
    if (PathUtils.hasPrefix(dstInodePath.getUri().getPath(), srcInodePath.getUri().getPath())) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_SUBDIRECTORY.getMessage(
          srcInodePath.getUri(), dstInodePath.getUri()));
    }

    // Get the inodes of the src and dst parents.
    Inode<?> srcParentInode = srcInodePath.getParentInodeDirectory();
    if (!srcParentInode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(srcInodePath.getUri()));
    }
    Inode<?> dstParentInode = dstInodePath.getParentInodeDirectory();
    if (!dstParentInode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(dstInodePath.getUri()));
    }

    // Make sure destination path does not exist
    if (dstInodePath.fullPathExists()) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(dstInodePath.getUri()));
    }

    // Now we remove srcInode from it's parent and insert it into dstPath's parent
    long opTimeMs = System.currentTimeMillis();
    renameInternal(srcInodePath, dstInodePath, false, opTimeMs);
    List<Inode<?>> persistedInodes = propagatePersistedInternal(srcInodePath, false);
    journalPersistedInodes(persistedInodes);

    RenameEntry rename = RenameEntry.newBuilder()
        .setId(srcInode.getId())
        .setDstPath(dstInodePath.getUri().getPath())
        .setOpTimeMs(opTimeMs)
        .build();
    return appendJournalEntry(JournalEntry.newBuilder().setRename(rename).build());
  }

  /**
   * Implements renaming.
   *
   * @param srcInodePath the path of the rename source
   * @param dstInodePath the path to the rename destionation
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  @GuardedBy("mInodeTree")
  void renameInternal(InodePath srcInodePath, InodePath dstInodePath, boolean replayed,
      long opTimeMs) throws FileDoesNotExistException, InvalidPathException, IOException {
    Inode<?> srcInode = srcInodePath.getInode();
    AlluxioURI srcPath = srcInodePath.getUri();
    AlluxioURI dstPath = dstInodePath.getUri();
    LOG.debug("Renaming {} to {}", srcPath, dstPath);

    // If the source file is persisted, rename it in the UFS.
    if (!replayed && srcInode.isPersisted()) {
      MountTable.Resolution resolution = mMountTable.resolve(srcPath);

      String ufsSrcUri = resolution.getUri().toString();
      UnderFileSystem ufs = resolution.getUfs();
      String ufsDstUri = mMountTable.resolve(dstPath).getUri().toString();
      String parentUri = new AlluxioURI(ufsDstUri).getParent().toString();
      if (!ufs.exists(parentUri) && !ufs.mkdirs(parentUri, true)) {
        throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(parentUri));
      }
      if (!ufs.rename(ufsSrcUri, ufsDstUri)) {
        throw new IOException(
            ExceptionMessage.FAILED_UFS_RENAME.getMessage(ufsSrcUri, ufsDstUri));
      }
    }

    // TODO(jiri): A crash between now and the time the rename operation is journaled will result in
    // an inconsistency between Alluxio and UFS.
    InodeDirectory srcParentInode = srcInodePath.getParentInodeDirectory();
    InodeDirectory dstParentInode = dstInodePath.getParentInodeDirectory();
    srcParentInode.removeChild(srcInode);
    srcParentInode.setLastModificationTimeMs(opTimeMs);
    srcInode.setParentId(dstParentInode.getId());
    srcInode.setName(dstPath.getName());
    dstParentInode.addChild(srcInode);
    dstParentInode.setLastModificationTimeMs(opTimeMs);
    MasterContext.getMasterSource().incPathsRenamed(1);
  }

  /**
   * @param entry the entry to use
   */
  @GuardedBy("mInodeTree")
  private void renameFromEntry(RenameEntry entry) {
    MasterContext.getMasterSource().incRenamePathOps(1);
    // Determine the srcPath and dstPath
    AlluxioURI srcPath;
    try (InodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.READ)) {
      srcPath = inodePath.getUri();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    AlluxioURI dstPath = new AlluxioURI(entry.getDstPath());

    try (InodePathPair inodePathPair = mInodeTree
        .lockInodePathPair(srcPath, InodeTree.LockMode.WRITE_PARENT, dstPath,
            InodeTree.LockMode.WRITE)) {
      InodePath srcInodePath = inodePathPair.getFirst();
      InodePath dstInodePath = inodePathPair.getSecond();
      renameInternal(srcInodePath, dstInodePath, true, entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * @param inodePath the inode to start the propagation at
   * @param replayed whether the invocation is a result of replaying the journal
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   */
  @GuardedBy("mInodeTree")
  private List<Inode<?>> propagatePersistedInternal(InodePath inodePath, boolean replayed)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    if (!inode.isPersisted()) {
      return Collections.emptyList();
    }

    List<Inode<?>> inodes = inodePath.getInodeList();
    // Traverse the inodes from target inode to the root.
    Collections.reverse(inodes);
    // Skip the first, to not examine the target inode itself.
    inodes = inodes.subList(1, inodes.size());

    List<Inode<?>> persistedInodes = new ArrayList<>();
    for (Inode<?> handle : inodes) {
      // the path is already locked.
      AlluxioURI path = mInodeTree.getPath(handle);
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
        persistedInodes.add(inode);
      }
    }
    return persistedInodes;
  }

  /**
   * Journals the list of persisted inodes returned from
   * {@link #propagatePersistedInternal(InodePath, boolean)}. This does not flush the journal.
   *
   * @param persistedInodes the list of persisted inodes to journal
   * @return the flush counter for journaling
   */
  private long journalPersistedInodes(List<Inode<?>> persistedInodes) {
    long counter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    for (Inode<?> inode : persistedInodes) {
      PersistDirectoryEntry persistDirectory =
          PersistDirectoryEntry.newBuilder().setId(inode.getId()).build();
      counter = appendJournalEntry(
          JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
    }
    return counter;
  }

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * <p>
   * This operation requires users to have {@link FileSystemAction#READ} permission on the path.
   *
   * @param path the path to free
   * @param recursive if true, and the file is a directory, all descendants will be freed
   * @return true if the file was freed
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  public boolean free(AlluxioURI path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incFreeFileOps(1);
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(FileSystemAction.READ, inodePath);
      return freeInternal(inodePath, recursive);
    }
  }

  /**
   * Implements free operation.
   *
   * @param inodePath inode of the path to free
   * @param recursive if true, and the file is a directory, all descendants will be freed
   * @return true if the file was freed
   */
  @GuardedBy("mInodeTree")
  private boolean freeInternal(InodePath inodePath, boolean recursive)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
      // true
      return false;
    }

    List<Inode<?>> freeInodes = new ArrayList<>();
    freeInodes.add(inode);

    try (InodeLockGroup lockGroup = mInodeTree
        .getInodeChildrenRecursive(inodePath, InodeTree.LockMode.READ)) {
      freeInodes.addAll(lockGroup.getInodes());

      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i--) {
        Inode<?> freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds(), false /* delete */);
        }
      }
    }

    MasterContext.getMasterSource().incFilesFreed(freeInodes.size());
    return true;
  }

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      // the path is already locked.
      return mInodeTree.getPath(inodePath.getInode());
    }
  }

  /**
   * @return the set of inode ids which are pinned
   */
  public Set<Long> getPinIdList() {
    return mInodeTree.getPinIdSet();
  }

  /**
   * @return the ufs address for this master
   */
  public String getUfsAddress() {
    return MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
  }

  /**
   * @return the white list
   */
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  /**
   * @return all the files lost on the workers
   */
  public List<Long> getLostFiles() {
    Set<Long> lostFiles = new HashSet<>();
    for (long blockId : mBlockMaster.getLostBlocks()) {
      // the file id is the container id of the block id
      long containerId = BlockId.getContainerId(blockId);
      long fileId = IdUtils.createFileId(containerId);
      lostFiles.add(fileId);
    }
    return new ArrayList<>(lostFiles);
  }

  /**
   * Reports a file as lost.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      Inode<?> inode = inodePath.getInode();
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory {}", inode);
        return;
      }

      List<Long> blockIds = Lists.newArrayList();
      try {
        for (FileBlockInfo fileBlockInfo : getFileBlockInfoListInternal(inodePath)) {
          blockIds.add(fileBlockInfo.getBlockInfo().getBlockId());
        }
      } catch (InvalidPathException e) {
        LOG.info("Failed to get file info {}", fileId, e);
      }
      mBlockMaster.reportLostBlocks(blockIds);
      LOG.info("Reported file loss of blocks {}. Alluxio will recompute it: {}", blockIds, fileId);
    }
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the path
   * and its parent path if path is a file, or @link FileSystemAction#WRITE} permission on the
   * parent path if path is a directory.
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
   * @throws AccessControlException if permission checking fails
   */
  // TODO(jiri): Make it possible to load UFS objects recursively.
  public long loadMetadata(AlluxioURI path, boolean recursive)
      throws BlockInfoException, FileAlreadyExistsException, FileDoesNotExistException,
      InvalidPathException, InvalidFileSizeException, FileAlreadyCompletedException, IOException,
      AccessControlException {
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      flushCounter = loadMetadataAndJournal(inodePath, recursive);
      return inodePath.getInode().getId();
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the path for which metadata should be loaded
   * @param recursive whether parent directories should be created if they do not already exist
   * @return the flush counter for journaling
   * @throws InvalidPathException if invalid path is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws FileAlreadyExistsException if the object to be loaded already exists
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws AccessControlException if permission checking fails
   * @throws IOException if an I/O error occurs
   */
  long loadMetadataAndJournal(InodePath inodePath, boolean recursive)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      BlockInfoException, FileAlreadyCompletedException, InvalidFileSizeException,
      AccessControlException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();
    try {
      if (!ufs.exists(ufsUri.toString())) {
        throw new FileDoesNotExistException(
            ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path.getPath()));
      }
      if (ufs.isFile(ufsUri.toString())) {
        long ufsBlockSizeByte = ufs.getBlockSizeByte(ufsUri.toString());
        long ufsLength = ufs.getFileSize(ufsUri.toString());
        // Metadata loaded from UFS has no TTL set.
        CreateFileOptions options =
            CreateFileOptions.defaults().setBlockSizeBytes(ufsBlockSizeByte).setRecursive(recursive)
                .setMetadataLoad(true).setPersisted(true);
        long counter = createFileAndJournal(inodePath, options);
        long fileId = inodePath.getInode().getId();
        CompleteFileOptions completeOptions =
            CompleteFileOptions.defaults().setUfsLength(ufsLength);
        counter = AsyncJournalWriter
            .getFlushCounter(counter, completeFileAndJournal(inodePath, completeOptions));
        return counter;
      }
      return loadDirectoryMetadataAndJournal(inodePath, recursive);
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   *
   * @param inodePath the path for which metadata should be loaded
   * @param recursive whether parent directories should be created if they do not already exist
   * @return the flush counter for journaling
   * @throws FileAlreadyExistsException if the object to be loaded already exists
   * @throws InvalidPathException if invalid path is encountered
   * @throws IOException if an I/O error occurs   *
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  @GuardedBy("mInodeTree")
  private long loadDirectoryMetadataAndJournal(InodePath inodePath, boolean recursive)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults()
        .setMountPoint(mMountTable.isMountPoint(inodePath.getUri())).setPersisted(true)
        .setRecursive(recursive).setMetadataLoad(true);
    long counter = createDirectoryAndJournal(inodePath, options);
    if (counter == AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(inodePath.getUri()));
    }
    return counter;
  }

  /**
   * Loads the metadata for the path, if it doesn't exist.
   *
   * @param inodePath the {@link InodePath} to load the metadata for
   */
  @GuardedBy("mInodeTree")
  private long loadMetadataIfNotExistAndJournal(InodePath inodePath) {
    if (!inodePath.fullPathExists()) {
      try {
        return loadMetadataAndJournal(inodePath, true);
      } catch (Exception e) {
        LOG.error("Failed to load metadata for path: {}", inodePath.getUri());
      }
    }
    return AsyncJournalWriter.INVALID_FLUSH_COUNTER;
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @throws FileAlreadyExistsException if the path to be mounted already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AccessControlException {
    MasterContext.getMasterSource().incMountOps(1);
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockInodePath(alluxioPath, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(alluxioPath);
      flushCounter = mountAndJournal(inodePath, ufsPath, options);
      MasterContext.getMasterSource().incPathsMounted(1);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @return the flush counter for journaling
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the path to be mounted already exists
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  long mountAndJournal(InodePath inodePath, AlluxioURI ufsPath, MountOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException {
    // Check that the Alluxio Path does not exist
    if (inodePath.fullPathExists()) {
      // TODO(calvin): Add a test to validate this (ALLUXIO-1831)
      throw new InvalidPathException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(inodePath.getUri()));
    }

    mountInternal(inodePath, ufsPath, options);
    boolean loadMetadataSuceeded = false;
    try {
      // This will create the directory at alluxioPath
      loadDirectoryMetadataAndJournal(inodePath, false);
      loadMetadataSuceeded = true;
    } catch (FileDoesNotExistException e) {
      // This exception should be impossible since we just mounted this path
      throw Throwables.propagate(e);
    } finally {
      if (!loadMetadataSuceeded) {
        unmountInternal(inodePath);
      }
      // Exception will be propagated from loadDirectoryMetadataAndJournal
    }

    // For proto, build a list of String pairs representing the properties map.
    Map<String, String> properties = options.getProperties();
    List<StringPairEntry> protoProperties = new ArrayList<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      protoProperties.add(StringPairEntry.newBuilder()
          .setKey(entry.getKey())
          .setValue(entry.getValue())
          .build());
    }

    AddMountPointEntry addMountPoint =
        AddMountPointEntry.newBuilder().setAlluxioPath(inodePath.getUri().toString())
            .setUfsPath(ufsPath.toString()).setReadOnly(options.isReadOnly())
            .addAllProperties(protoProperties).build();
    return appendJournalEntry(JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build());
  }

  /**
   * @param entry the entry to use
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  @GuardedBy("mInodeTree")
  void mountFromEntry(AddMountPointEntry entry)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    AlluxioURI ufsURI = new AlluxioURI(entry.getUfsPath());
    try (InodePath inodePath = mInodeTree.lockInodePath(alluxioURI, InodeTree.LockMode.WRITE)) {
      mountInternal(inodePath, ufsURI, new MountOptions(entry));
    }
  }

  /**
   * Updates the mount table with the specified mount point. The mount options may be updated during
   * this method.
   *
   * @param inodePath the Alluxio mount point
   * @param ufsPath the UFS endpoint to mount
   * @param options the mount options (may be updated)
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  @GuardedBy("mInodeTree")
  void mountInternal(InodePath inodePath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioPath = inodePath.getUri();
    // Check that the ufsPath exists and is a directory
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath.toString(), MasterContext.getConf());
    ufs.setProperties(options.getProperties());
    if (!ufs.exists(ufsPath.toString())) {
      throw new IOException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
    }
    if (ufs.isFile(ufsPath.toString())) {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_DIRECTORY.getMessage(ufsPath.getPath()));
    }
    // Check that the alluxioPath we're creating doesn't shadow a path in the default UFS
    String defaultUfsPath = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem defaultUfs = UnderFileSystem.get(defaultUfsPath, MasterContext.getConf());
    if (defaultUfs.exists(PathUtils.concatPath(defaultUfsPath, alluxioPath.getPath()))) {
      throw new IOException(
          ExceptionMessage.MOUNT_PATH_SHADOWS_DEFAULT_UFS.getMessage(alluxioPath));
    }
    // This should check that we are not mounting a prefix of an existing mount, and that no
    // existing mount is a prefix of this mount.
    mMountTable.add(alluxioPath, ufsPath, options);

    try {
      // Configure the ufs properties, and update the mount options with the configured properties.
      ufs.configureProperties();
      options.setProperties(ufs.getProperties());
    } catch (IOException e) {
      // remove the mount point if the UFS failed to configure properties.
      mMountTable.delete(alluxioPath);
      throw e;
    }
  }

  /**
   * Unmounts a UFS path previously mounted path onto an Alluxio path.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to unmount, must be a mount point
   * @return true if the UFS path was successfully unmounted, false otherwise
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  public boolean unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    MasterContext.getMasterSource().incUnmountOps(1);
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (
        InodePath inodePath = mInodeTree.lockFullInodePath(alluxioPath, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(FileSystemAction.WRITE, inodePath);
      flushCounter = unmountAndJournal(inodePath);
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        MasterContext.getMasterSource().incPathsUnmounted(1);
        return true;
      }
      return false;
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Unmounts a UFS path previously mounted path onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the Alluxio path to unmount, must be a mount point
   * @return the flush counter for journaling
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws IOException if an I/O error occurs
   */
  long unmountAndJournal(InodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException, IOException {
    if (unmountInternal(inodePath)) {
      Inode<?> inode = inodePath.getInode();
      // Use the internal delete API, setting {@code replayed} to true to prevent the delete
      // operations from being persisted in the UFS.
      long fileId = inode.getId();
      long opTimeMs = System.currentTimeMillis();
      deleteRecursiveInternal(inodePath, true /* replayed */, opTimeMs);
      DeleteFileEntry deleteFile =
          DeleteFileEntry.newBuilder().setId(fileId).setRecursive(true).setOpTimeMs(opTimeMs)
              .build();
      appendJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build());
      DeleteMountPointEntry deleteMountPoint =
          DeleteMountPointEntry.newBuilder().setAlluxioPath(inodePath.getUri().toString()).build();
      return appendJournalEntry(
          JournalEntry.newBuilder().setDeleteMountPoint(deleteMountPoint).build());
    }
    return AsyncJournalWriter.INVALID_FLUSH_COUNTER;
  }

  /**
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if path does not exist
   */
  @GuardedBy("mInodeTree")
  private void unmountFromEntry(DeleteMountPointEntry entry)
      throws InvalidPathException, FileDoesNotExistException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    try (InodePath inodePath = mInodeTree.lockFullInodePath(alluxioURI, InodeTree.LockMode.WRITE)) {
      if (!unmountInternal(inodePath)) {
        LOG.error("Failed to unmount {}", alluxioURI);
      }
    }
  }

  /**
   * @param inodePath the Alluxio mount point to unmount
   * @return true if successful, false otherwise
   * @throws InvalidPathException if an invalied path is encountered
   */
  @GuardedBy("mInodeTree")
  private boolean unmountInternal(InodePath inodePath) throws InvalidPathException {
    return mMountTable.delete(inodePath.getUri());
  }

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid for the id of the file
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public void resetFile(long fileId)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    // TODO(yupeng) check the file is not persisted
    try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
      // free the file first
      InodeFile inodeFile = inodePath.getInodeFile();
      freeInternal(inodePath, false);
      inodeFile.reset();
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * This operation requires users to have {@link FileSystemAction#WRITE} permission on the path. In
   * addition, the client user must be a super user when setting the owner, and must be a super user
   * or the owner when setting the group or permission.
   *
   * @param path the path to set attribute for
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    MasterContext.getMasterSource().incSetAttributeOps(1);
    // for chown
    boolean rootRequired = options.getOwner() != null;
    // for chgrp, chmod
    boolean ownerRequired =
        (options.getGroup() != null) || (options.getPermission() != Constants.INVALID_PERMISSION);
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
      flushCounter = setAttributeAndJournal(inodePath, options, rootRequired, ownerRequired);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link InodePath} to set attribute for
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @throws InvalidPathException if the given path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   */
  long setAttributeAndJournal(InodePath inodePath, SetAttributeOptions options,
      boolean rootRequired, boolean ownerRequired)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException {
    Inode<?> targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (options.isRecursive() && targetInode.isDirectory()) {
      try (InodeLockGroup lockGroup = mInodeTree
          .getInodeChildrenRecursive(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodeChildren = lockGroup.getInodes();
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          try (InodePath childPath = mInodeTree.lockFullInodePath(mInodeTree.getPath(inode),
              InodeTree.LockMode.READ)) {
            // TODO(gpang): a better way to check permissions
            mPermissionChecker
                .checkSetAttributePermission(childPath, rootRequired, ownerRequired);
          }
        }
        TempInodePathWithDescendant tempInodePath = new TempInodePathWithDescendant(inodePath);
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          tempInodePath.setDescendant(inode, mInodeTree.getPath(inode));
          List<Inode<?>> persistedInodes = setAttributeInternal(tempInodePath, opTimeMs, options);
          journalPersistedInodes(persistedInodes);
          journalSetAttribute(inode.getId(), opTimeMs, options);
        }
      }
    }
    List<Inode<?>> persistedInodes = setAttributeInternal(inodePath, opTimeMs, options);
    journalPersistedInodes(persistedInodes);
    return journalSetAttribute(targetInode.getId(), opTimeMs, options);
  }

  /**
   * @param fileId the file id to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @return the flush counter for journaling
   */
  @GuardedBy("mInodeTree")
  private long journalSetAttribute(long fileId, long opTimeMs, SetAttributeOptions options) {
    SetAttributeEntry.Builder builder =
        SetAttributeEntry.newBuilder().setId(fileId).setOpTimeMs(opTimeMs);
    if (options.getPinned() != null) {
      builder.setPinned(options.getPinned());
    }
    if (options.getTtl() != null) {
      builder.setTtl(options.getTtl());
    }
    if (options.getPersisted() != null) {
      builder.setPersisted(options.getPersisted());
    }
    if (options.getOwner() != null) {
      builder.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      builder.setGroup(options.getGroup());
    }
    if (options.getPermission() != Constants.INVALID_PERMISSION) {
      builder.setPermission(options.getPermission());
    }
    return appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build());
  }

  /**
   * Schedules a file for async persistence.
   *
   * @param path the id of the file for persistence
   * @throws AlluxioException if scheduling fails
   */
  public void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    long flushCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    try (InodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      flushCounter = scheduleAsyncPersistenceAndJournal(inodePath);
    } finally {
      // finally runs after resources are closed (unlocked).
      if (flushCounter != AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
        waitForJournalFlush(flushCounter);
      }
    }
    // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
    mAsyncPersistHandler.scheduleAsyncPersistence(path);
  }

  /**
   * Schedules a file for async persistence.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link InodePath} of the file for persistence
   * @return the flush counter for journaling
   * @throws AlluxioException if scheduling fails
   */
  long scheduleAsyncPersistenceAndJournal(InodePath inodePath) throws AlluxioException {
    long fileId = inodePath.getInode().getId();
    scheduleAsyncPersistenceInternal(inodePath);
    // write to journal
    AsyncPersistRequestEntry asyncPersistRequestEntry =
        AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
    return appendJournalEntry(
        JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build());
  }

  /**
   * @param inodePath the {@link InodePath} of the file to schedule asynchronous persistence for
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceInternal(InodePath inodePath) throws AlluxioException {
    inodePath.getInode().setPersistenceState(PersistenceState.IN_PROGRESS);
  }

  /**
   * Instructs a worker to persist the files.
   * <p>
   * Needs {@link FileSystemAction#WRITE} permission on the list of files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    for (long fileId : persistedFiles) {
      // Permission checking for each file is performed inside setAttribute
      setAttribute(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
    }

    // get the files for the given worker to persist
    List<PersistFile> filesToPersist = mAsyncPersistHandler.pollFilesToPersist(workerId);
    if (!filesToPersist.isEmpty()) {
      LOG.debug("Sent files {} to worker {} to persist", filesToPersist, workerId);
    }
    FileSystemCommandOptions options = new FileSystemCommandOptions();
    options.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, options);
  }

  /**
   * @param inodePath the {@link InodePath} to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException
   */
  @GuardedBy("mInodeTree")
  List<Inode<?>> setAttributeInternal(InodePath inodePath, long opTimeMs,
      SetAttributeOptions options)
      throws FileDoesNotExistException {
    List<Inode<?>> persistedInodes = Collections.emptyList();
    Inode<?> inode = inodePath.getInode();
    if (options.getPinned() != null) {
      mInodeTree.setPinned(inodePath, options.getPinned(), opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    if (options.getTtl() != null) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.TTL_ONLY_FOR_FILE);
      long ttl = options.getTtl();
      InodeFile file = (InodeFile) inode;
      if (file.getTtl() != ttl) {
        mTtlBuckets.remove(file);
        file.setTtl(ttl);
        mTtlBuckets.insert(file);
        file.setLastModificationTimeMs(opTimeMs);
      }
    }
    if (options.getPersisted() != null) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(((InodeFile) inode).isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      InodeFile file = (InodeFile) inode;
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions.checkArgument(options.getPersisted(),
          PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!file.isPersisted()) {
        file.setPersistenceState(PersistenceState.PERSISTED);
        persistedInodes = propagatePersistedInternal(inodePath, false);
        file.setLastModificationTimeMs(opTimeMs);
        MasterContext.getMasterSource().incFilesPersisted(1);
      }
    }
    if (options.getOwner() != null) {
      inode.setUserName(options.getOwner());
    }
    if (options.getGroup() != null) {
      inode.setGroupName(options.getGroup());
    }
    if (options.getPermission() != Constants.INVALID_PERMISSION) {
      inode.setPermission(options.getPermission());
    }
    return persistedInodes;
  }

  /**
   * @param entry the entry to use
   * @throws FileDoesNotExistException if the file does not exist
   */
  @GuardedBy("mInodeTree")
  private void setAttributeFromEntry(SetAttributeEntry entry) throws FileDoesNotExistException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    if (entry.hasPinned()) {
      options.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      options.setTtl(entry.getTtl());
    }
    if (entry.hasPersisted()) {
      options.setPersisted(entry.getPersisted());
    }
    if (entry.hasOwner()) {
      options.setOwner(entry.getOwner());
    }
    if (entry.hasGroup()) {
      options.setGroup(entry.getGroup());
    }
    if (entry.hasPermission()) {
      options.setPermission((short) entry.getPermission());
    }
    try (InodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      setAttributeInternal(inodePath, entry.getOpTimeMs(), options);
      // Intentionally not journaling the persisted inodes from setAttributeInternal
    }
  }

  /**
   * This class represents the executor for periodic inode ttl check.
   */
  private final class MasterInodeTtlCheckExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
      for (TtlBucket bucket : expiredBuckets) {
        for (InodeFile file : bucket.getFiles()) {
          AlluxioURI path = null;
          try (InodePath inodePath = mInodeTree
              .lockFullInodePath(file.getId(), InodeTree.LockMode.READ)) {
            path = inodePath.getUri();
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check: {}", file.toString(),
                e.toString());
          }
          if (path != null) {
            try {
              // public delete method will lock the path, and check WRITE permission required at
              // parent of file
              delete(path, false);
            } catch (Exception e) {
              LOG.error("Exception trying to clean up {} for ttl check: {}", file.toString(),
                  e.toString());
            }
          }
        }
      }
      mTtlBuckets.removeBuckets(expiredBuckets);
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Lost files periodic check.
   */
  private final class LostFilesDetectionHeartbeatExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      for (long fileId : getLostFiles()) {
        // update the state
        try (InodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          Inode<?> inode = inodePath.getInode();
          if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
            inode.setPersistenceState(PersistenceState.LOST);
          }
        } catch (FileDoesNotExistException e) {
          LOG.error("Exception trying to get inode from inode tree: {}", e.toString());
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
