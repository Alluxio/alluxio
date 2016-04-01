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
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.authorization.PermissionStatus;
import alluxio.security.group.GroupMappingService;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemCommandOptions;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.PersistCommandOptions;
import alluxio.thrift.PersistFile;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

  /** This provides user groups mapping service. */
  private final GroupMappingService mGroupMappingService;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

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

    mWorkerToAsyncPersistFiles = Maps.newHashMap();
    mGroupMappingService = GroupMappingService.Factory.getUserToGroupsMappingService(conf);
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
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      } catch (InvalidFileSizeException e) {
        throw new RuntimeException(e);
      } catch (FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (innerEntry instanceof SetAttributeEntry) {
      try {
        setAttributeFromEntry((SetAttributeEntry) innerEntry);
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
    } else if (innerEntry instanceof AsyncPersistRequestEntry) {
      try {
        long fileId = ((AsyncPersistRequestEntry) innerEntry).getFileId();
        scheduleAsyncPersistenceInternal(getPath(fileId));
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      } catch (InvalidPathException e) {
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
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(defaultUFS),
            MountOptions.defaults());
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
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTtlCheckExecutor(),
              MasterContext.getConf().getInt(Constants.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_FILES_DETECTION, new LostFilesDetectionHeartbeatExecutor(),
          MasterContext.getConf().getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS)));
    }
  }

  /**
   * Whether the filesystem contains a directory with the id.
   *
   * @param id id of the directory
   * @return true if there is a directory with the id, false otherwise
   */
  public boolean isDirectory(long id) {
    synchronized (mInodeTree) {
      Inode<?> inode;
      if (!mInodeTree.inodeIdExists(id)) {
        return false;
      }
      try {
        inode = mInodeTree.getInodeById(id);
      } catch (FileDoesNotExistException e) {
        return false;
      }
      return inode.isDirectory();
    }
  }

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   * Needs {@link FileSystemAction#READ} permission on the path.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  public long getFileId(AlluxioURI path) throws AccessControlException, FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode<?> inode;
      checkPermission(FileSystemAction.READ, path, false);
      if (!mInodeTree.inodePathExists(path)) {
        try {
          return loadMetadata(path, true);
        } catch (Exception e) {
          return IdUtils.INVALID_FILE_ID;
        }
      }
      try {
        inode = mInodeTree.getInodeByPath(path);
      } catch (InvalidPathException e) {
        return IdUtils.INVALID_FILE_ID;
      }
      return inode.getId();
    }
  }

  /**
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path. Called via RPC, as well as internal masters.
   * Needs {@link FileSystemAction#READ} permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  public FileInfo getFileInfo(AlluxioURI path)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileInfoOps(1);
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.READ, path, false);
      // getFileInfo should load from ufs if the file does not exist
      getFileId(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);
      return getFileInfoInternal(inode);
    }
  }

  /**
   * @param fileId the file id
   * @return the persistence state for the given file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
      return inode.getPersistenceState();
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param inode the inode to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   */
  private FileInfo getFileInfoInternal(Inode<?> inode) throws FileDoesNotExistException {
    FileInfo fileInfo = inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    AlluxioURI path = mInodeTree.getPath(inode);
    AlluxioURI resolvedPath;
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
   * Returns a list {@link FileInfo} for a given path. If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * Needs {@link FileSystemAction#READ} permission on the path.
   * If the path is a directory, needs {@link FileSystemAction#EXECUTE} permission on the path.
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
      checkPermission(FileSystemAction.READ, path, false);
      // getFileInfoList should load from ufs if the file does not exist
      getFileId(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);

      List<FileInfo> ret = new ArrayList<FileInfo>();
      if (inode.isDirectory()) {
        checkPermission(FileSystemAction.EXECUTE, path, false);
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
   * Needs {@link FileSystemAction#WRITE} permission on the path.
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
      checkPermission(FileSystemAction.WRITE, path, false);
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param blockIds the block ids to use
   * @param fileId the file id to use
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
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
   * Creates a file (not a directory) for a given path. Needs {@link FileSystemAction#WRITE}
   * permission on the parent of the path.
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
  public long create(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
          BlockInfoException, IOException, FileDoesNotExistException {
    MasterContext.getMasterSource().incCreateFileOps(1);
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, path, true);
      if (!options.isMetadataLoad()) {
        mMountTable.checkUnderWritableMountPoint(path);
      }
      InodeTree.CreatePathResult createResult = createInternal(path, options);
      List<Inode<?>> created = createResult.getCreated();

      writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
      journalCreatePathResult(createResult);
      flushJournal();
      return created.get(created.size() - 1).getId();
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
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
  InodeTree.CreatePathResult createInternal(AlluxioURI path, CreateFileOptions options)
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   */
  private void resetBlockFileFromEntry(ReinitializeFileEntry entry) {
    try {
      mInodeTree.reinitializeFile(new AlluxioURI(entry.getPath()), entry.getBlockSizeBytes(),
          entry.getTtl());
    } catch (InvalidPathException | FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Since {@link FileSystemMaster#create(AlluxioURI, CreateFileOptions)} already checked
   * {@link alluxio.security.authorization.FileSystemAction#WRITE},
   * it is not needed to check again here when requesting a new block for the file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   */
  public long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetNewBlockOps(1);
    Inode<?> inode;
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
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the path.
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
  public boolean deleteFile(AlluxioURI path, boolean recursive)
      throws IOException, FileDoesNotExistException, DirectoryNotEmptyException,
          InvalidPathException, AccessControlException {
    MasterContext.getMasterSource().incDeletePathOps(1);
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, path, true);
      mMountTable.checkUnderWritableMountPoint(path);
      Inode<?> inode = mInodeTree.getInodeByPath(path);
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

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   */
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
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
        // Remove corresponding blocks from workers and delete metadata in master.
        mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds(), true /* delete */);
      }

      mInodeTree.deleteInode(delInode, opTimeMs);
    }
    MasterContext.getMasterSource().incPathsDeleted(delInodes.size());
    return true;
  }

  /**
   * Returns the {@link FileBlockInfo} for given file and block index.
   *
   * @param fileId the file id to get the info for
   * @param fileBlockIndex the block index of the file to get the block info for
   * @return the {@link FileBlockInfo} for the file and block index
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if the block size is invalid
   * @throws InvalidPathException if the mount table is not able to resolve the file
   */
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileBlockInfoOps(1);
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
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
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path of the given file is invalid
   */
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileBlockInfoOps(1);
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeByPath(path);
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param file the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   * @throws InvalidPathException if the mount table is not able to resolve the file
   */
  private FileBlockInfo generateFileBlockInfo(InodeFile file, BlockInfo blockInfo)
      throws InvalidPathException {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setUfsLocations(new ArrayList<WorkerNetAddress>());

    // The sequence number part of the block id is the block index.
    long offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.getBlockId());
    fileBlockInfo.setOffset(offset);

    if (fileBlockInfo.getBlockInfo().getLocations().isEmpty() && file.isPersisted()) {
      // No alluxio locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      String ufsPath = mMountTable.resolve(mInodeTree.getPath(file)).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
      List<String> locs;
      try {
        locs = ufs.getFileLocations(ufsPath, fileBlockInfo.getOffset());
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
          } catch (NumberFormatException e) {
            continue;
          }
          // The resolved port is the data transfer port not the rpc port
          fileBlockInfo.getUfsLocations().add(
              new WorkerNetAddress().setHost(resolvedHost).setDataPort(resolvedPort));
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
    Queue<Pair<InodeDirectory, AlluxioURI>> nodesQueue =
        new LinkedList<Pair<InodeDirectory, AlluxioURI>>();
    synchronized (mInodeTree) {
      // TODO(yupeng): Verify we want to use absolute path.
      nodesQueue.add(new Pair<InodeDirectory, AlluxioURI>(mInodeTree.getRoot(),
          new AlluxioURI(AlluxioURI.SEPARATOR)));
      while (!nodesQueue.isEmpty()) {
        Pair<InodeDirectory, AlluxioURI> pair = nodesQueue.poll();
        InodeDirectory directory = pair.getFirst();
        AlluxioURI curUri = pair.getSecond();

        Set<Inode<?>> children = directory.getChildren();
        for (Inode<?> inode : children) {
          AlluxioURI newUri = curUri.join(inode.getName());
          if (inode.isDirectory()) {
            nodesQueue.add(new Pair<InodeDirectory, AlluxioURI>((InodeDirectory) inode, newUri));
          } else if (isFullyInMemory((InodeFile) inode)) {
            ret.add(newUri);
          }
        }
      }
    }
    return ret;
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
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param options method options
   * @return an {@link alluxio.master.file.meta.InodeTree.CreatePathResult} representing the
   *         modified inodes and created inodes during path creation
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath(AlluxioURI, CreatePathOptions)} for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public InodeTree.CreatePathResult mkdir(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    LOG.debug("mkdir {} ", path);
    MasterContext.getMasterSource().incCreateDirectoriesOps(1);
    synchronized (mInodeTree) {
      try {
        checkPermission(FileSystemAction.WRITE, path, true);
        if (!options.isMetadataLoad()) {
          mMountTable.checkUnderWritableMountPoint(path);
        }
        InodeTree.CreatePathResult createResult = mInodeTree.createPath(path, options);

        LOG.debug("writing journal entry for mkdir {}", path);
        writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
        journalCreatePathResult(createResult);
        flushJournal();
        LOG.debug("flushed journal for mkdir {}", path);
        MasterContext.getMasterSource().incDirectoriesCreated(1);
        return createResult;
      } catch (BlockInfoException e) {
        // Since we are creating a directory, the block size is ignored, no such exception should
        // happen.
        Throwables.propagate(e);
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
    for (Inode<?> inode : createResult.getModified()) {
      InodeLastModificationTimeEntry inodeLastModificationTime =
          InodeLastModificationTimeEntry.newBuilder()
          .setId(inode.getId())
          .setLastModificationTimeMs(inode.getLastModificationTimeMs())
          .build();
      writeJournalEntry(JournalEntry.newBuilder()
          .setInodeLastModificationTime(inodeLastModificationTime).build());
    }
    for (Inode<?> inode : createResult.getCreated()) {
      writeJournalEntry(inode.toJournalEntry());
    }
    for (Inode<?> inode : createResult.getPersisted()) {
      PersistDirectoryEntry persistDirectory = PersistDirectoryEntry.newBuilder()
          .setId(inode.getId())
          .build();
      writeJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
    }
  }

  /**
   * Renames a file to a destination.
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the src path.
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the dst path.
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
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, srcPath, true);
      checkPermission(FileSystemAction.WRITE, dstPath, true);
      mMountTable.checkUnderWritableMountPoint(srcPath);
      mMountTable.checkUnderWritableMountPoint(dstPath);
      Inode<?> srcInode = mInodeTree.getInodeByPath(srcPath);
      // Renaming path to itself is a no-op.
      if (srcPath.equals(dstPath)) {
        return;
      }
      // Renaming the root is not allowed.
      if (srcPath.isRoot()) {
        throw new InvalidPathException(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage());
      }
      if (dstPath.isRoot()) {
        throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage());
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
        throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_SUBDIRECTORY.getMessage(
            srcPath, dstPath));
      }

      AlluxioURI dstParentURI = dstPath.getParent();

      // Get the inodes of the src and dst parents.
      Inode<?> srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
      if (!srcParentInode.isDirectory()) {
        throw new InvalidPathException(
            ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(srcPath));
      }
      Inode<?> dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
      if (!dstParentInode.isDirectory()) {
        throw new InvalidPathException(
            ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(dstPath));
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
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param fileId the file id of the rename source
   * @param dstPath the path to the rename destionation
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  void renameInternal(long fileId, AlluxioURI dstPath, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    Inode<?> srcInode = mInodeTree.getInodeById(fileId);
    AlluxioURI srcPath = mInodeTree.getPath(srcInode);
    LOG.debug("Renaming {} to {}", srcPath, dstPath);

    // If the source file is persisted, rename it in the UFS.
    FileInfo fileInfo = getFileInfoInternal(srcInode);
    if (!replayed && fileInfo.isPersisted()) {
      String ufsSrcPath = mMountTable.resolve(srcPath).toString();
      String ufsDstPath = mMountTable.resolve(dstPath).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsSrcPath, MasterContext.getConf());
      String parentPath = new AlluxioURI(ufsDstPath).getParent().toString();
      if (!ufs.exists(parentPath) && !ufs.mkdirs(parentPath, true)) {
        throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(parentPath));
      }
      if (!ufs.rename(ufsSrcPath, ufsDstPath)) {
        throw new IOException(
            ExceptionMessage.FAILED_UFS_RENAME.getMessage(ufsSrcPath, ufsDstPath));
      }
    }

    // TODO(jiri): A crash between now and the time the rename operation is journaled will result in
    // an inconsistency between Alluxio and UFS.
    Inode<?> srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
    AlluxioURI dstParentURI = dstPath.getParent();
    Inode<?> dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
    ((InodeDirectory) srcParentInode).removeChild(srcInode);
    srcParentInode.setLastModificationTimeMs(opTimeMs);
    srcInode.setParentId(dstParentInode.getId());
    srcInode.setName(dstPath.getName());
    ((InodeDirectory) dstParentInode).addChild(srcInode);
    dstParentInode.setLastModificationTimeMs(opTimeMs);
    MasterContext.getMasterSource().incPathsRenamed(1);
    propagatePersisted(srcInode, replayed);
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   */
  private void renameFromEntry(RenameEntry entry) {
    MasterContext.getMasterSource().incRenamePathOps(1);
    try {
      renameInternal(entry.getId(), new AlluxioURI(entry.getDstPath()), true,
          entry.getOpTimeMs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param inode the inode to start the propagation at
   * @param replayed whether the invocation is a result of replaying the journal
   * @throws FileDoesNotExistException if a non-existent file is encountered
   */
  private void propagatePersisted(Inode<?> inode, boolean replayed)
      throws FileDoesNotExistException {
    if (!inode.isPersisted()) {
      return;
    }
    Inode<?> handle = inode;
    while (handle.getParentId() != InodeTree.NO_PARENT) {
      handle = mInodeTree.getInodeById(handle.getParentId());
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
        PersistDirectoryEntry persistDirectory = PersistDirectoryEntry.newBuilder()
            .setId(inode.getId())
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
      }
    }
  }

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * Needs {@link FileSystemAction#WRITE} permission on the path.
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
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, path, false);

      Inode<?> inode = mInodeTree.getInodeByPath(path);

      if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
        // true
        return false;
      }

      List<Inode<?>> freeInodes = new ArrayList<Inode<?>>();
      freeInodes.add(inode);
      if (inode.isDirectory()) {
        freeInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
      }

      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i--) {
        Inode<?> freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds(), false /* delete */);
        }
      }
      MasterContext.getMasterSource().incFilesFreed(freeInodes.size());
    }
    return true;
  }

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      return mInodeTree.getPath(mInodeTree.getInodeById(fileId));
    }
  }

  /**
   * @return the set of inode ids which are pinned
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
   * @return the white list
   */
  public List<String> getWhiteList() {
    synchronized (mInodeTree) {
      return mWhitelist.getList();
    }
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

  /**
   * Reports a file as lost.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode<?> inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory {}", inode);
        return;
      }

      List<Long> blockIds = Lists.newArrayList();
      try {
        for (FileBlockInfo fileBlockInfo : getFileBlockInfoList(getPath(fileId))) {
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
   * Needs {@link FileSystemAction#READ} permission on the path.
   * Implicitly needs {@link FileSystemAction#WRITE} permission on the parent of the path.
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
    AlluxioURI ufsPath;
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.READ, path, false);
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
        CreateFileOptions options =
            CreateFileOptions.defaults().setBlockSizeBytes(ufsBlockSizeByte).setRecursive(recursive)
                .setMetadataLoad(true).setPersisted(true);
        long fileId = create(path, options);
        CompleteFileOptions completeOptions =
            CompleteFileOptions.defaults().setUfsLength(ufsLength);
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
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   *
   * @param path the path for which metadata should be loaded
   * @param recursive whether parent directories should be created if they do not already exist
   * @return the file id of the loaded directory
   * @throws FileAlreadyExistsException if the object to be loaded already exists
   * @throws InvalidPathException if invalid path is encountered
   * @throws IOException if an I/O error occurs   *
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  private long loadMetadataDirectory(AlluxioURI path, boolean recursive)
      throws IOException, FileAlreadyExistsException, InvalidPathException, AccessControlException,
      FileDoesNotExistException {
    CreateDirectoryOptions options =
        CreateDirectoryOptions.defaults().setMountPoint(mMountTable.isMountPoint(path))
            .setPersisted(true).setRecursive(recursive).setMetadataLoad(true);
    InodeTree.CreatePathResult result = mkdir(path, options);
    List<Inode<?>> inodes = null;
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
   * Mounts a UFS path onto an Alluxio path.
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the Tachyon path.
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
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, alluxioPath, true);
      mMountTable.checkUnderWritableMountPoint(alluxioPath);
      mountInternal(alluxioPath, ufsPath, options);
      boolean loadMetadataSuceeded = false;
      try {
        // This will create the directory at alluxioPath
        loadMetadataDirectory(alluxioPath, false);
        loadMetadataSuceeded = true;
      } catch (FileDoesNotExistException e) {
        // This exception should be impossible since we just mounted this path
        throw Throwables.propagate(e);
      } finally {
        if (!loadMetadataSuceeded) {
          unmountInternal(alluxioPath);
        }
        // Exception will be propagated from loadMetadataDirectory
      }
      AddMountPointEntry addMountPoint =
          AddMountPointEntry.newBuilder().setAlluxioPath(alluxioPath.toString())
              .setUfsPath(ufsPath.toString()).setReadOnly(options.isReadOnly()).build();
      writeJournalEntry(JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build());
      flushJournal();
      MasterContext.getMasterSource().incPathsMounted(1);
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  void mountFromEntry(AddMountPointEntry entry)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    AlluxioURI ufsURI = new AlluxioURI(entry.getUfsPath());
    mountInternal(alluxioURI, ufsURI, new MountOptions(entry));
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param alluxioPath the Alluxio mount point
   * @param ufsPath the UFS endpoint to mount
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  void mountInternal(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    // Check that the ufsPath exists and is a directory
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath.toString(), MasterContext.getConf());
    if (!ufs.exists(ufsPath.getPath())) {
      throw new IOException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
    }
    if (ufs.isFile(ufsPath.getPath())) {
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
  }

  /**
   * Unmounts a UFS path previously mounted path onto an Alluxio path.
   * Needs {@link FileSystemAction#WRITE} permission on the parent of the Tachyon path.
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
    synchronized (mInodeTree) {
      checkPermission(FileSystemAction.WRITE, alluxioPath, true);
      if (unmountInternal(alluxioPath)) {
        Inode<?> inode = mInodeTree.getInodeByPath(alluxioPath);
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
            .setAlluxioPath(alluxioPath.toString())
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setDeleteMountPoint(deleteMountPoint).build());
        flushJournal();
        MasterContext.getMasterSource().incPathsUnmounted(1);
        return true;
      }
    }
    return false;
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   */
  void unmountFromEntry(DeleteMountPointEntry entry) throws InvalidPathException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    if (!unmountInternal(alluxioURI)) {
      LOG.error("Failed to unmount {}", alluxioURI);
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param alluxioPath the Alluxio mount point to unmount
   * @return true if successful, false otherwise
   * @throws InvalidPathException if an invalied path is encountered
   */
  boolean unmountInternal(AlluxioURI alluxioPath) throws InvalidPathException {
    return mMountTable.delete(alluxioPath);
  }

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   * Implicitly needs {@link FileSystemAction#WRITE} permission on the path indicated by file id.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid for the id of the file
   */
  public void resetFile(long fileId)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    // TODO(yupeng) check the file is not persisted
    synchronized (mInodeTree) {
      // free the file first
      free(getPath(fileId), false);
      InodeFile inodeFile = (InodeFile) mInodeTree.getInodeById(fileId);
      inodeFile.reset();
    }
  }

  /**
   * Sets the file attribute.
   * Needs {@link FileSystemAction#WRITE} permission on the path.
   * The client user needs to be a super user when setting owner.
   * The client user needs to be a super user or path owner when setting group or permission.
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
    synchronized (mInodeTree) {
      checkSetAttributePermission(path, rootRequired, ownerRequired);

      long fileId = mInodeTree.getInodeByPath(path).getId();
      long opTimeMs = System.currentTimeMillis();
      Inode<?> targetInode = mInodeTree.getInodeByPath(path);
      if (options.isRecursive() && targetInode.isDirectory()) {
        List<Inode<?>> inodeChildren =
            mInodeTree.getInodeChildrenRecursive((InodeDirectory) targetInode);
        for (Inode<?> inode : inodeChildren) {
          checkSetAttributePermission(mInodeTree.getPath(inode), rootRequired, ownerRequired);
        }
        for (Inode<?> inode : inodeChildren) {
          long id = inode.getId();
          setAttributeInternal(id, opTimeMs, options);
          journalSetAttribute(id, opTimeMs, options);
        }
      }
      setAttributeInternal(fileId, opTimeMs, options);
      journalSetAttribute(fileId, opTimeMs, options);
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param fileId the file id to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   */
  private void journalSetAttribute(long fileId, long opTimeMs, SetAttributeOptions options) {
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
      builder.setPermission(options.getPermission().shortValue());
    }
    writeJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build());
    flushJournal();
  }

  /**
   * Schedules a file for async persistence.
   *
   * @param path the id of the file for persistence
   * @return the id of the worker that persistence is scheduled on
   * @throws FileDoesNotExistException when the file does not exist
   * @throws InvalidPathException if the given path is invalid
   */
  public long scheduleAsyncPersistence(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException {
    synchronized (mInodeTree) {
      long workerId = scheduleAsyncPersistenceInternal(path);
      long fileId = mInodeTree.getInodeByPath(path).getId();
      // write to journal
      AsyncPersistRequestEntry asyncPersistRequestEntry =
          AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
      writeJournalEntry(
          JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build());
      flushJournal();
      return workerId;
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param path the path to schedule asynchronous persistence for
   * @return the id of the worker that will perform the operation
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   */
  private long scheduleAsyncPersistenceInternal(AlluxioURI path) throws
      FileDoesNotExistException, InvalidPathException {
    // find the worker
    long workerId = getWorkerStoringFile(path);

    if (workerId == IdUtils.INVALID_WORKER_ID) {
      LOG.warn("No worker found to schedule async persistence for file {}", path);
      // no worker found, do nothing
      return workerId;
    }

    // update the state
    Inode<?> inode = mInodeTree.getInodeByPath(path);
    inode.setPersistenceState(PersistenceState.IN_PROGRESS);
    long fileId = inode.getId();

    if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
      mWorkerToAsyncPersistFiles.put(workerId, Sets.<Long>newHashSet());
    }
    mWorkerToAsyncPersistFiles.get(workerId).add(fileId);

    return workerId;
  }

  /**
   * Gets a worker where the given file is stored.
   *
   * @param path the path to the file
   * @return the id of the storing worker
   * @throws FileDoesNotExistException when the file does not exist on any worker
   */
  // TODO(calvin): Propagate the exceptions in certain cases
  private long getWorkerStoringFile(AlluxioURI path) throws FileDoesNotExistException {
    Map<Long, Integer> workerBlockCounts = Maps.newHashMap();
    List<FileBlockInfo> blockInfoList;
    try {
      blockInfoList = getFileBlockInfoList(path);

      for (FileBlockInfo fileBlockInfo : blockInfoList) {
        for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
          if (workerBlockCounts.containsKey(blockLocation.getWorkerId())) {
            workerBlockCounts.put(blockLocation.getWorkerId(),
                workerBlockCounts.get(blockLocation.getWorkerId()) + 1);
          } else {
            workerBlockCounts.put(blockLocation.getWorkerId(), 1);
          }

          // TODO(yupeng) remove the requirement that all the blocks of a file must be stored on the
          // same worker, for now it returns the first worker that has all the blocks
          if (workerBlockCounts.get(blockLocation.getWorkerId()) == blockInfoList.size()) {
            return blockLocation.getWorkerId();
          }
        }
      }
    } catch (FileDoesNotExistException e) {
      LOG.error("The file {} to persist does not exist", path);
      return IdUtils.INVALID_WORKER_ID;
    } catch (InvalidPathException e) {
      LOG.error("The file {} to persist is invalid", path);
      return IdUtils.INVALID_WORKER_ID;
    }

    if (workerBlockCounts.size() == 0) {
      LOG.error("The file " + path + " does not exist on any worker");
      return IdUtils.INVALID_WORKER_ID;
    }

    LOG.error("Not all the blocks of file {} stored on the same worker", path);
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
            blockIds.add(fileBlockInfo.getBlockInfo().getBlockId());
          }

          filesToPersist.add(new PersistFile(fileId, blockIds));
          // update the inode file persisence state
          inode.setPersistenceState(PersistenceState.IN_PROGRESS);
        }
      }
      mWorkerToAsyncPersistFiles.get(workerId).removeAll(fileIdsToPersist);
    }
    return filesToPersist;
  }

  /**
   * Instructs a worker to persist the files.
   * Implicitly needs {@link FileSystemAction#WRITE} permission on the path.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
  public synchronized FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    for (long fileId : persistedFiles) {
      setAttribute(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
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

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param fileId the file id to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @throws FileDoesNotExistException
   */
  private void setAttributeInternal(long fileId, long opTimeMs, SetAttributeOptions options)
      throws FileDoesNotExistException {
    Inode<?> inode = mInodeTree.getInodeById(fileId);
    if (options.getPinned() != null) {
      mInodeTree.setPinned(inode, options.getPinned(), opTimeMs);
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
        propagatePersisted(file, false);
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
      inode.setPermission(options.getPermission().shortValue());
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param entry the entry to use
   * @throws FileDoesNotExistException if the file does not exist
   */
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
    setAttributeInternal(entry.getId(), entry.getOpTimeMs(), options);
  }

  /**
   * Checks whether the client user is the owner of the path.
   *
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param path to be checked on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  private void checkOwner(AlluxioURI path) throws AccessControlException, InvalidPathException {
    // bypasses permission checking if security is not enabled.
    if (!SecurityUtils.isSecurityEnabled(MasterContext.getConf())) {
      return;
    }

    // collects inodes info on the path
    List<FileInfo> fileInfos = collectFileInfoList(path);

    // collects user and groups
    String user = getClientUser();
    List<String> groups = getGroups(user);

    // checks the owner
    PermissionChecker.checkOwner(user, groups, path, fileInfos);
  }

  /**
   * Checks whether a user has permission to edit the attribute of a given path.
   *
   * @param path the path to check permission on
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  private void checkSetAttributePermission(AlluxioURI path, boolean rootRequired, boolean
      ownerRequired) throws AccessControlException, InvalidPathException {
    // bypasses permission checking if security is not enabled.
    if (!SecurityUtils.isSecurityEnabled(MasterContext.getConf())) {
      return;
    }
    // For chown, superuser is required
    if (rootRequired) {
      PermissionChecker.checkSuperuser(getClientUser(), getGroups(getClientUser()));
    }
    // For chgrp or chmod, owner is required
    if (ownerRequired) {
      checkOwner(path);
    }
    checkPermission(FileSystemAction.WRITE, path, false);
  }

  /**
   * Checks whether a user has permission to perform a specific action on a path. If the path is
   * invalid, it should bypass the {@link InvalidPathException}.
   *
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @param checkParent indicates whether to check its parent
   * @throws AccessControlException if permission checking fails
   */
  private void checkPermission(FileSystemAction action, AlluxioURI path, boolean checkParent)
      throws AccessControlException {
    // bypasses permission checking if security is not enabled.
    if (!SecurityUtils.isSecurityEnabled(MasterContext.getConf())) {
      return;
    }

    try {
      // collects inodes info on the path
      List<FileInfo> fileInfos = collectFileInfoList(path);

      // collects user and groups
      String user = getClientUser();
      List<String> groups = getGroups(user);

      // perform permission check
      String[] pathComponents = PathUtils.getPathComponents(path.getPath());
      if (checkParent) {
        if (// involved methods: create, mkdir, rename, deleteFile
            action.equals(FileSystemAction.WRITE)
            // create or mkdir under root "/", then assumes to have write permission.
            && (fileInfos.size() == 1 && pathComponents.length > 1)
            // rename or deleteFile under root "/", then must be the owner.
            || (fileInfos.size() == 2 && pathComponents.length == 2)) {
          // Handle a special case where the path is a level under root "/" and checking write
          // permission on it. We simply assume user has write permission on the root "/",
          // with a limitation that the user must be the owner of the path.
          PermissionChecker.checkOwner(user, groups, path, fileInfos);
        } else {
          PermissionChecker.checkParentPermission(user, groups, action, path, fileInfos);
        }
      } else {
        PermissionChecker.checkPermission(user, groups, action, path, fileInfos);
      }
    } catch (InvalidPathException e) {
      LOG.warn("Invalid Path {} for checking permission: " + e.getMessage(), path);
    }
  }

  /**
   * @return the client user
   * @throws AccessControlException if the client user information cannot be accessed
   */
  private String getClientUser() throws AccessControlException {
    try {
      User authorizedUser = AuthenticatedClientUser.get(MasterContext.getConf());
      if (authorizedUser == null) {
        throw new AccessControlException(
            ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
      }
      return authorizedUser.getName();
    } catch (IOException e) {
      throw new AccessControlException(e.getMessage());
    }
  }

  /**
   * @param user the user to get groups for
   * @return the groups for the given user
   * @throws AccessControlException if the group service information cannot be accessed
   */
  private List<String> getGroups(String user) throws AccessControlException {
    try {
      return mGroupMappingService.getGroups(user);
    } catch (IOException e) {
      throw new AccessControlException(
          ExceptionMessage.PERMISSION_DENIED.getMessage(e.getMessage()));
    }
  }

  /**
   * NOTE: {@link #mInodeTree} should already be locked before calling this method.
   *
   * @param path the path to collect file information for
   * @return a list of {@link FileInfo}
   * @throws InvalidPathException if an invalid path is encountered
   */
  private List<FileInfo> collectFileInfoList(AlluxioURI path) throws InvalidPathException {
    List<FileInfo> fileInfos = Lists.newArrayList();
    for (Inode<?> inodeOnPath :  mInodeTree.collectInodes(path)) {
      fileInfos.add(inodeOnPath.generateClientFileInfo(mInodeTree.getPath(inodeOnPath).toString()));
    }
    return fileInfos;
  }

  /**
   * This class represents the executor for periodic inode ttl check.
   */
  private final class MasterInodeTtlCheckExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      synchronized (mInodeTree) {
        Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
        for (TtlBucket bucket : expiredBuckets) {
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
        mTtlBuckets.removeBuckets(expiredBuckets);
      }
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
        synchronized (mInodeTree) {
          Inode<?> inode;
          try {
            inode = mInodeTree.getInodeById(fileId);
            if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
              inode.setPersistenceState(PersistenceState.LOST);
            }
          } catch (FileDoesNotExistException e) {
            LOG.error("Exception trying to get inode from inode tree: {}", e.toString());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
