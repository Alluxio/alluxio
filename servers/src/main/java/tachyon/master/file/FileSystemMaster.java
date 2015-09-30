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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.StorageLevelAlias;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockId;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.journal.AddMountPointEntry;
import tachyon.master.file.journal.CompleteFileEntry;
import tachyon.master.file.journal.DeleteFileEntry;
import tachyon.master.file.journal.DeleteMountPointEntry;
import tachyon.master.file.journal.DependencyEntry;
import tachyon.master.file.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.file.journal.InodeEntry;
import tachyon.master.file.journal.InodeLastModificationTimeEntry;
import tachyon.master.file.journal.PersistDirectoryEntry;
import tachyon.master.file.journal.PersistFileEntry;
import tachyon.master.file.journal.RenameEntry;
import tachyon.master.file.journal.SetPinnedEntry;
import tachyon.master.file.meta.Dependency;
import tachyon.master.file.meta.DependencyMap;
import tachyon.master.file.meta.Inode;
import tachyon.master.file.meta.InodeDirectory;
import tachyon.master.file.meta.InodeDirectoryIdGenerator;
import tachyon.master.file.meta.InodeFile;
import tachyon.master.file.meta.InodeTree;
import tachyon.master.file.meta.MountTable;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.io.PathUtils;

/**
 * The master that handles all file system metadata management.
 */
public final class FileSystemMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockMaster mBlockMaster;
  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;
  /** This manages metadata for lineage. This must be journaled. */
  private final DependencyMap mDependencyMap = new DependencyMap();
  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;
  /** This manages the file system mount points. */
  private final MountTable mMountTable = new MountTable();
  private final PrefixList mWhitelist;

  /** The service that tries to check inodefiles with ttl set */
  private Future<?> mTTLCheckerService;
  /** The configuration of {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS} */
  private int mTTLCheckerIntervalMs;

  /**
   * A bucket with all files whose ttl value lies in the bucket's time interval. The bucket's time
   * interval starts at a specific time and lasts for {@link #mTTLCheckerIntervalMs}.
   *
   * Not thread-safe. Only for use related to {@link TTLBucketList}.
   */
  private class TTLBucket {
    /**
     * Each bucket has a time to live interval, this value is the start of the interval, interval
     * value is the same as the configuration of {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS}.
     */
    private long mTTLIntervalStartTimeMs;
    /** A list of InodeFiles whose ttl value is in the range of this bucket's interval. */
    private List<InodeFile> mFiles;

    public TTLBucket(long startTimeMs) {
      mTTLIntervalStartTimeMs = startTimeMs;
      mFiles = new LinkedList<InodeFile>();
    }

    public long getTTLIntervalStartTimeMs() {
      return mTTLIntervalStartTimeMs;
    }

    public void setTTLIntervalStartMs(long startTimeMs) {
      mTTLIntervalStartTimeMs = startTimeMs;
    }

    public long getTTLIntervalEndTimeMs() {
      return mTTLIntervalStartTimeMs + mTTLCheckerIntervalMs;
    }

    public List<InodeFile> getFiles() {
      return mFiles;
    }

    public void addFile(InodeFile file) {
      mFiles.add(file);
    }
  }

  /**
   * A list of non-empty {@link TTLBucket}s sorted by ttl interval start time of each bucket.
   *
   * Two adjacent buckets may not have adjacent intervals since there may be no files with ttl value
   * in the skipped intervals.
   *
   * Thread-safety is guaranteed by {@link ConcurrentSkipListSet}.
   */
  private class TTLBucketList {
    /**
     * List of buckets sorted by interval start time.
     * SkipList is used for O(logn) insertion and retrieval, see {@link ConcurrentSkipListSet}.
     */
    private ConcurrentSkipListSet<TTLBucket> mBucketList;

    public TTLBucketList() {
      mBucketList = new ConcurrentSkipListSet<TTLBucket>(new Comparator<TTLBucket>() {
        @Override
        public int compare(TTLBucket ttlBucket, TTLBucket ttlBucket2) {
          long startTime1 = ttlBucket.getTTLIntervalStartTimeMs();
          long startTime2 = ttlBucket2.getTTLIntervalStartTimeMs();
          if (startTime1 < startTime2) {
            return -1;
          }
          if (startTime1 == startTime2) {
            return 0;
          }
          return 1;
        }
      });
    }

    /**
     * Inserts an {@link InodeFile} to the appropriate bucket where its ttl end time lies in the
     * bucket's interval, if no appropriate bucket exists, a new bucket will be created to contain
     * this file, if ttl value is {@link Constants#NO_TTL}, the file won't be inserted to any
     * buckets and nothing will happen.
     *
     * @param file the file to be inserted
     */
    public void insert(InodeFile file) {
      if (file.getTTL() == Constants.NO_TTL) {
        return;
      }

      long ttlEndTimeMs = file.getCreationTimeMs() + file.getTTL();
      // Gets the last bucket with interval start time less than or equal to the file's life end
      // time.
      TTLBucket bucket = mBucketList.floor(new TTLBucket(ttlEndTimeMs));
      if (bucket == null || bucket.getTTLIntervalEndTimeMs() <= ttlEndTimeMs) {
        // 1. There is no bucket in the list.
        // 2. All buckets' interval start time is larger than the file's life end time.
        // 3. No bucket actually contains ttlEndTimeMs in its interval.
        // So a new bucket should should be added with ttlEndTimeMs as its interval start.
        bucket = new TTLBucket(ttlEndTimeMs);
        mBucketList.add(bucket);
      }
      bucket.addFile(file);
    }

    /**
     * Retrieves buckets whose ttl interval has expired before the specified time, that is, the
     * bucket's interval start time should be less than or equal to (specified time - ttl interval).
     * The returned set is backed by the internal set.
     *
     * @param time the expiration time
     * @return a set of expired buckets or an empty set if no buckets have expired
     */
    public Set<TTLBucket> getExpiredBuckets(long time) {
      return mBucketList.headSet(new TTLBucket(time - mTTLCheckerIntervalMs), true);
    }

    /**
     * Remove all buckets in the set.
     *
     * @param buckets a set of buckets to be removed
     */
    public void removeBuckets(Set<TTLBucket> buckets) {
      mBucketList.removeAll(buckets);
    }
  }

  private final TTLBucketList mTTLBuckets = new TTLBucketList();

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.FILE_SYSTEM_MASTER_SERVICE_NAME);
  }

  public FileSystemMaster(BlockMaster blockMaster, Journal journal) {
    super(journal, Executors.newFixedThreadPool(2,
        ThreadFactoryUtils.build("file-system-master-%d", true)));
    mBlockMaster = blockMaster;

    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator);

    // TODO(gene): Handle default config value for whitelist.
    TachyonConf conf = MasterContext.getConf();
    mWhitelist = new PrefixList(conf.getList(Constants.MASTER_WHITELIST, ","));
  }

  @Override
  public TProcessor getProcessor() {
    return new FileSystemMasterService.Processor<FileSystemMasterServiceHandler>(
        new FileSystemMasterServiceHandler(this));
  }

  @Override
  public String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry instanceof InodeEntry) {
      mInodeTree.addInodeFromJournal((InodeEntry) entry);
    } else if (entry instanceof InodeLastModificationTimeEntry) {
      InodeLastModificationTimeEntry modTimeEntry = (InodeLastModificationTimeEntry) entry;
      try {
        Inode inode = mInodeTree.getInodeById(modTimeEntry.getId());
        inode.setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry instanceof PersistDirectoryEntry) {
      PersistDirectoryEntry typedEntry = (PersistDirectoryEntry) entry;
      try {
        Inode inode = mInodeTree.getInodeById(typedEntry.getId());
        inode.setPersisted(typedEntry.isPersisted());
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry instanceof DependencyEntry) {
      DependencyEntry dependencyEntry = (DependencyEntry) entry;
      Dependency dependency =
          new Dependency(dependencyEntry.mId, dependencyEntry.mParentFiles,
              dependencyEntry.mChildrenFiles, dependencyEntry.mCommandPrefix,
              dependencyEntry.mData, dependencyEntry.mComment, dependencyEntry.mFramework,
              dependencyEntry.mFrameworkVersion, dependencyEntry.mDependencyType,
              dependencyEntry.mParentDependencies, dependencyEntry.mCreationTimeMs);
      for (int childDependencyId : dependencyEntry.mChildrenDependencies) {
        dependency.addChildrenDependency(childDependencyId);
      }
      for (long lostFileId : dependencyEntry.mLostFileIds) {
        dependency.addLostFile(lostFileId);
      }
      dependency.resetUncheckpointedChildrenFiles(dependencyEntry.mUncheckpointedFiles);
      mDependencyMap.addDependency(dependency);
    } else if (entry instanceof CompleteFileEntry) {
      try {
        completeFileFromEntry((CompleteFileEntry) entry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry instanceof PersistFileEntry) {
      persistFileFromEntry((PersistFileEntry) entry);
    } else if (entry instanceof SetPinnedEntry) {
      setPinnedFromEntry((SetPinnedEntry) entry);
    } else if (entry instanceof DeleteFileEntry) {
      deleteFileFromEntry((DeleteFileEntry) entry);
    } else if (entry instanceof RenameEntry) {
      renameFromEntry((RenameEntry) entry);
    } else if (entry instanceof InodeDirectoryIdGeneratorEntry) {
      mDirectoryIdGenerator.fromJournalEntry((InodeDirectoryIdGeneratorEntry) entry);
    } else if (entry instanceof AddMountPointEntry) {
      try {
        mountFromEntry((AddMountPointEntry) entry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry instanceof DeleteMountPointEntry) {
      try {
        unmountFromEntry((DeleteMountPointEntry) entry);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECETD_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    mInodeTree.streamToJournalCheckpoint(outputStream);
    mDependencyMap.streamToJournalCheckpoint(outputStream);
    outputStream.writeEntry(mDirectoryIdGenerator.toJournalEntry());
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    if (isLeader) {
      // Only initialize root when isLeader because when initializing root, BlockMaster needs to
      // write journal entry, if it is not leader, BlockMaster won't have a writable journal.
      // If it is standby, it should be able to load the inode tree from leader's checkpoint.
      TachyonConf conf = MasterContext.getConf();
      mInodeTree.initializeRoot();
      String defaultUFS = conf.get(Constants.UNDERFS_DATA_FOLDER);
      try {
        mMountTable.add(new TachyonURI(MountTable.ROOT), new TachyonURI(defaultUFS));
      } catch (InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
      mTTLCheckerIntervalMs = conf.getInt(Constants.MASTER_TTLCHECKER_INTERVAL_MS);
      mTTLCheckerService =
          getExecutorService().submit(
              new HeartbeatThread("InodeFile TTL Check", new MasterInodeTTLCheckExecutor(),
                  mTTLCheckerIntervalMs));
    }
    super.start(isLeader);
  }

  @Override
  public void stop() throws IOException {
    super.stop();
    if (mTTLCheckerService != null) {
      mTTLCheckerService.cancel(true);
    }
  }

  /**
   * Persists a file in UFS.
   *
   * @param fileId the file id
   * @param length the length of the file
   * @return true on success
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   * @throws FileDoesNotExistException
   */
  public boolean persistFile(long fileId, long length) throws SuspectedFileSizeException,
      BlockInfoException, FileDoesNotExistException {
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      if (persistFileInternal(fileId, length, opTimeMs)) {
        writeJournalEntry(new PersistFileEntry(fileId, length, opTimeMs));
        flushJournal();
      }
    }
    return true;
  }

  /**
   * Internal implementation of persisting a file to ufs.
   *
   * @return true if the operation should be written to the journal
   */
  boolean persistFileInternal(long fileId, long length, long opTimeMs)
      throws SuspectedFileSizeException, BlockInfoException, FileDoesNotExistException {

    Inode inode = mInodeTree.getInodeById(fileId);
    if (inode.isDirectory()) {
      throw new FileDoesNotExistException(ExceptionMessage.FILEID_MUST_BE_FILE.getMessage(fileId));
    }

    InodeFile file = (InodeFile) inode;
    boolean needLog = false;

    if (file.isCompleted()) {
      if (file.getLength() != length) {
        throw new SuspectedFileSizeException(fileId + ". Original Size: " + file.getLength()
            + ". New Size: " + length);
      }
    } else {
      file.setLength(length);
      // Commit all the file blocks (without locations) so the metadata for the block exists.
      long currLength = length;
      for (long blockId : file.getBlockIds()) {
        long blockSize = Math.min(currLength, file.getBlockSizeBytes());
        mBlockMaster.commitBlockInUFS(blockId, blockSize);
        currLength -= blockSize;
      }

      needLog = true;
    }

    if (!file.isPersisted()) {
      file.setPersisted(true);
      needLog = true;

      Dependency dep = mDependencyMap.getFromFileId(fileId);
      if (dep != null) {
        dep.childCheckpointed(fileId);
        if (dep.hasCheckpointed()) {
          mDependencyMap.removeUncheckpointedDependency(dep);
          mDependencyMap.removePriorityDependency(dep);
        }
      }
      mDependencyMap.addFileCheckpoint(fileId);
    }
    file.setLastModificationTimeMs(opTimeMs);
    file.setCompleted(length);
    MasterContext.getMasterSource().incFilesCheckpointed();
    // TODO(calvin): This probably should always be true since the last mod time is updated.
    return needLog;
  }

  private void persistFileFromEntry(PersistFileEntry entry) {
    try {
      persistFileInternal(entry.getFileId(), entry.getFileLength(), entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    } catch (SuspectedFileSizeException sfse) {
      throw new RuntimeException(sfse);
    } catch (BlockInfoException bie) {
      throw new RuntimeException(bie);
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
   * Returns the file id for a given path. Called via RPC, as well as internal masters.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path
   * @throws InvalidPathException
   */
  public long getFileId(TachyonURI path) throws InvalidPathException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      return inode.getId();
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path. Called via RPC, as well as internal masters.
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException
   */
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException, InvalidPathException {
    MasterContext.getMasterSource().incGetFileStatusOps();
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      return getFileInfoInternal(inode);
    }
  }

  private FileInfo getFileInfoInternal(Inode inode) throws FileDoesNotExistException,
      InvalidPathException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    FileInfo fileInfo = inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    fileInfo.inMemoryPercentage = getInMemoryPercentage(inode);
    TachyonURI path = mInodeTree.getPath(inode);
    TachyonURI resolvedPath = mMountTable.resolve(path);
    // Only set the UFS path if the path is nested under a mount point.
    if (!path.equals(resolvedPath)) {
      fileInfo.setUfsPath(resolvedPath.toString());
    }
    return fileInfo;
  }

  /**
   * Returns a list {@link FileInfo} for a given file id. If the given file id is a file, the list
   * only contains a single object. If it is a directory, the resulting list contains all direct
   * children of the directory. Called via RPC, as well as internal masters.
   *
   * @param fileId
   * @return
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public List<FileInfo> getFileInfoList(long fileId) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);

      List<FileInfo> ret = new ArrayList<FileInfo>();
      if (inode.isDirectory()) {
        for (Inode child : ((InodeDirectory) inode).getChildren()) {
          ret.add(getFileInfoInternal(child));
        }
      } else {
        ret.add(getFileInfoInternal(inode));
      }
      return ret;
    }
  }

  /**
   * Marks a file as completed. After a file is complete, it cannot be written to. Called via RPC.
   *
   * @param fileId the file id to complete.
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  public void completeFile(long fileId) throws BlockInfoException, FileDoesNotExistException,
      InvalidPathException {
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      Inode inode = mInodeTree.getInodeById(fileId);
      if (!inode.isFile()) {
        throw new FileDoesNotExistException(
            ExceptionMessage.FILEID_MUST_BE_FILE.getMessage(fileId));
      }

      InodeFile fileInode = (InodeFile) inode;
      List<Long> blockIdList = fileInode.getBlockIds();
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
      if (blockInfoList.size() != blockIdList.size()) {
        throw new BlockInfoException("Cannot complete file without all the blocks committed");
      }

      // Verify that all the blocks (except the last one) is the same size as the file block size.
      long fileLength = 0;
      long fileBlockSize = fileInode.getBlockSizeBytes();
      for (int i = 0; i < blockInfoList.size(); i ++) {
        BlockInfo blockInfo = blockInfoList.get(i);
        fileLength += blockInfo.getLength();
        if (i < blockInfoList.size() - 1 && blockInfo.getLength() != fileBlockSize) {
          throw new BlockInfoException("Block index " + i
              + " has a block size smaller than the file block size ("
              + fileInode.getBlockSizeBytes() + ")");
        }
      }

      completeFileInternal(fileInode.getBlockIds(), fileId, fileLength, false, opTimeMs);
      writeJournalEntry(
          new CompleteFileEntry(fileInode.getBlockIds(), fileId, fileLength, opTimeMs));
      flushJournal();
    }
  }

  void completeFileInternal(List<Long> blockIds, long fileId, long fileLength, boolean replayed,
      long opTimeMs) throws FileDoesNotExistException, InvalidPathException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    mDependencyMap.addFileCheckpoint(fileId);
    InodeFile inodeFile = (InodeFile) mInodeTree.getInodeById(fileId);
    inodeFile.setBlockIds(blockIds);
    inodeFile.setCompleted(fileLength);
    inodeFile.setLastModificationTimeMs(opTimeMs);
    // Mark all parent directories that have a UFS counterpart as persisted.
    propagatePersisted(inodeFile, replayed);
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * @param inode the inode to start the propagation at
   * @param replayed whether the invocation is a result of replaying the journal
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void propagatePersisted(Inode inode, boolean replayed) throws FileDoesNotExistException,
      InvalidPathException {
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
      handle.setPersisted(true);
      if (!replayed) {
        writeJournalEntry(new PersistDirectoryEntry(inode.getId(), inode.isPersisted()));
      }
    }
  }

  private void completeFileFromEntry(CompleteFileEntry entry) throws InvalidPathException {
    try {
      completeFileInternal(entry.getBlockIds(), entry.getFileId(), entry.getFileLength(), true,
          entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }

  /**
   * Creates a file (not a directory) for a given path. Called via RPC.
   *
   * @param path the file to create
   * @param blockSizeBytes the block size of the file
   * @param recursive if true, will recursively create all the missing directories along the path.
   * @return the file id of the create file
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws BlockInfoException
   */
  public long createFile(TachyonURI path, long blockSizeBytes, boolean recursive)
      throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    return createFile(path, blockSizeBytes, recursive, Constants.NO_TTL);
  }

  /**
   * Creates a file (not a directory) for a given path. Called via RPC.
   *
   * @param path the file to create
   * @param blockSizeBytes the block size of the file
   * @param recursive if true, will recursively create all the missing directories along the path.
   * @param ttl time to live for file
   * @return the file id of the create file
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws BlockInfoException
   */
  public long createFile(TachyonURI path, long blockSizeBytes, boolean recursive, long ttl)
      throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    MasterContext.getMasterSource().incCreateFileOps();
    synchronized (mInodeTree) {
      InodeTree.CreatePathResult createResult =
          createFileInternal(path, blockSizeBytes, recursive, System.currentTimeMillis(), ttl);
      List<Inode> created = createResult.getCreated();

      writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
      journalCreatePathResult(createResult);
      flushJournal();
      return created.get(created.size() - 1).getId();
    }
  }

  InodeTree.CreatePathResult createFileInternal(TachyonURI path, long blockSizeBytes,
      boolean recursive, long opTimeMs, long ttl) throws InvalidPathException,
      FileAlreadyExistException, BlockInfoException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    InodeTree.CreatePathResult createResult =
        mInodeTree.createPath(path, blockSizeBytes, recursive, false, opTimeMs, ttl);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(path.toString())) {
      inode.setCacheable(true);
    }

    mTTLBuckets.insert(inode);

    MasterContext.getMasterSource().incFilesCreated(created.size());
    return createResult;
  }

  /**
   * Returns the next block id for a given file id. Called via RPC.
   *
   * @param fileId the file id to get the next block id for
   * @return the next block id for the file
   * @throws FileDoesNotExistException
   */
  public long getNewBlockIdForFile(long fileId) throws FileDoesNotExistException {
    Inode inode;
    synchronized (mInodeTree) {
      inode = mInodeTree.getInodeById(fileId);
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.FILEID_MUST_BE_FILE.getMessage(fileId));
    }

    return ((InodeFile) inode).getNewBlockId();
  }

  /**
   * Get the total number of files and directories.
   *
   * @return the number of files and directories.
   */
  public int getNumberOfFiles() {
    synchronized (mInodeTree) {
      return mInodeTree.getSize();
    }
  }

  /**
   * Get the number of pinned files and directories.
   * @return the number of pinned files and directories.
   */
  public int getNumberOfPinnedFiles() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinnedSize();
    }
  }

  /**
   * Deletes a given file id. Called via RPC.
   *
   * @param fileId the file id to delete
   * @param recursive if true, will delete all its children.
   * @return true if the file was deleted, false otherwise.
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  public boolean deleteFile(long fileId, boolean recursive) throws FileDoesNotExistException,
      InvalidPathException, IOException {
    MasterContext.getMasterSource().incDeleteFileOps();
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      TachyonURI path = mInodeTree.getPath(mInodeTree.getInodeById(fileId));
      boolean ret = deleteFileInternal(fileId, recursive, false, opTimeMs);
      writeJournalEntry(new DeleteFileEntry(fileId, recursive, opTimeMs));
      flushJournal();
      return ret;
    }
  }

  private void deleteFileFromEntry(DeleteFileEntry entry) {
    MasterContext.getMasterSource().incDeleteFileOps();
    try {
      deleteFileInternal(entry.mFileId, entry.mRecursive, true, entry.mOpTimeMs);
    } catch (Exception e) {
      throw new RuntimeException(e);
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
   * @return
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  boolean deleteFileInternal(long fileId, boolean recursive, boolean replayed,
      long opTimeMs) throws FileDoesNotExistException, InvalidPathException, IOException {
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
      return false;
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
        String ufsPath = mMountTable.resolve(mInodeTree.getPath(delInode)).toString();
        UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
        if (!ufs.exists(ufsPath)) {
          LOG.warn("File does not exist the underfs: " + ufsPath);
        } else if (!ufs.delete(ufsPath, true)) {
          LOG.error("Failed to delete " + ufsPath);
          return false;
        }
      }

      if (delInode.isFile()) {
        // Remove corresponding blocks from workers.
        mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds());
      }

      mInodeTree.deleteInode(delInode, opTimeMs);
    }
    MasterContext.getMasterSource().incFilesDeleted(delInodes.size());
    return true;
  }

  /**
   * Returns the {@link FileBlockInfo} for given file and block index. Called via RPC.
   *
   * @param fileId the file id to get the info for
   * @param fileBlockIndex the block index of the file to get the block info for
   * @return the {@link FileBlockInfo} for the file and block index
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException {
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
        throw new BlockInfoException("FileId " + fileId + " BlockIndex " + fileBlockIndex
            + " is not a valid block.");
      }
      return generateFileBlockInfo(file, blockInfoList.get(0));
    }
  }

  /**
   * Returns all the {@link FileBlockInfo} of the given file. Called via RPC, and internal masters.
   *
   * @param fileId the file id to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the file.
   * @throws FileDoesNotExistException
   */
  public List<FileBlockInfo> getFileBlockInfoList(long fileId) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException(
                ExceptionMessage.FILEID_MUST_BE_FILE.getMessage(fileId));
      }
      InodeFile file = (InodeFile) inode;
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(file.getBlockIds());

      List<FileBlockInfo> ret = new ArrayList<FileBlockInfo>();
      for (BlockInfo blockInfo : blockInfoList) {
        ret.add(generateFileBlockInfo(file, blockInfo));
      }
      return ret;
    }
  }

  /**
   * Returns all the {@link FileBlockInfo} of the given file. Called by web UI.
   *
   * @param path the path to the file
   * @return a list of {@link FileBlockInfo} for all the blocks of the file.
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public List<FileBlockInfo> getFileBlockInfoList(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    long fileId = getFileId(path);
    return getFileBlockInfoList(fileId);
  }

  /**
   * Generates a {@link FileBlockInfo} object from internal metadata. This adds file information to
   * the block, such as the file offset, and additional UFS locations for the block.
   *
   * @param file the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   */
  private FileBlockInfo generateFileBlockInfo(InodeFile file, BlockInfo blockInfo) throws
      InvalidPathException {
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
          fileBlockInfo.ufsLocations.add(new NetAddress(resolvedHost, -1, resolvedPort));
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
   * @return absolute paths of all in memory files.
   */
  public List<TachyonURI> getInMemoryFiles() {
    List<TachyonURI> ret = new ArrayList<TachyonURI>();
    Queue<Pair<InodeDirectory, TachyonURI>> nodesQueue =
        new LinkedList<Pair<InodeDirectory, TachyonURI>>();
    synchronized (mInodeTree) {
      // TODO(yupeng): Verify we want to use absolute path.
      nodesQueue.add(new Pair<InodeDirectory, TachyonURI>(mInodeTree.getRoot(), new TachyonURI(
          TachyonURI.SEPARATOR)));
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
      if (isInMemory(info)) {
        inMemoryLength += info.getLength();
      }
    }
    return (int) (inMemoryLength * 100 / length);
  }

  /**
   * @return true if the given block is in some worker's memory, false otherwise
   */
  private boolean isInMemory(BlockInfo blockInfo) {
    for (BlockLocation location : blockInfo.getLocations()) {
      if (location.getTier() == StorageLevelAlias.MEM.getValue()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates a directory for a given path. Called via RPC, and internal masters.
   *
   * @param path the path of the directory
   * @param recursive if it is true, create necessary but nonexistent parent directories, otherwise,
   *        the parent directories must already exist
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath} for more details
   * @throws FileAlreadyExistException when there is already a file at path
   */
  public InodeTree.CreatePathResult mkdir(TachyonURI path, boolean recursive)
      throws InvalidPathException, FileAlreadyExistException {
    // TODO(gene): metrics
    synchronized (mInodeTree) {
      try {
        InodeTree.CreatePathResult createResult = mInodeTree.createPath(path, 0, recursive, true);

        writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
        journalCreatePathResult(createResult);
        flushJournal();
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
      writeJournalEntry(new InodeLastModificationTimeEntry(inode.getId(),
          inode.getLastModificationTimeMs()));
    }
    for (Inode inode : createResult.getCreated()) {
      writeJournalEntry(inode.toJournalEntry());
    }
  }

  /**
   * Renames a file to a destination. Called via RPC.
   *
   * @param fileId the source file to rename.
   * @param dstPath the destination path to rename the file to.
   * @return true if the rename was successful
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   */
  public boolean rename(long fileId, TachyonURI dstPath) throws FileAlreadyExistException,
      FileDoesNotExistException, InvalidPathException, IOException {
    MasterContext.getMasterSource().incRenameOps();
    synchronized (mInodeTree) {
      Inode srcInode = mInodeTree.getInodeById(fileId);
      TachyonURI srcPath = mInodeTree.getPath(srcInode);
      // Renaming path to itself is a no-op.
      if (srcPath.equals(dstPath)) {
        return true;
      }
      // Renaming the root is not allowed.
      if (srcPath.isRoot() || dstPath.isRoot()) {
        return false;
      }
      // Renaming across mount points is not allowed.
      String srcMount = mMountTable.getMountPoint(srcPath);
      String dstMount = mMountTable.getMountPoint(dstPath);
      if ((srcMount == null && dstMount != null)
          || (srcMount != null && dstMount == null)
          || (srcMount != null && dstMount != null && !srcMount.equals(dstMount))) {
        LOG.warn("Renaming " + srcPath + " to " + dstPath + " spans mount points.");
        return false;
      }
      // Renaming onto a mount point is not allowed.
      if (mMountTable.isMountPoint(dstPath)) {
        return false;
      }
      // Renaming a path to one of its subpaths is not allowed. Check for that, by making sure
      // srcComponents isn't a prefix of dstComponents.
      if (PathUtils.hasPrefix(dstPath.getPath(), srcPath.getPath())) {
        throw new InvalidPathException("Failed to rename: " + srcPath + " is a prefix of "
            + dstPath);
      }

      TachyonURI dstParentURI = dstPath.getParent();

      // Get the inodes of the src and dst parents.
      Inode srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
      if (!srcParentInode.isDirectory()) {
        return false;
      }
      Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
      if (!dstParentInode.isDirectory()) {
        return false;
      }

      // Make sure destination path does not exist
      InodeDirectory dstParentDirectory = (InodeDirectory) dstParentInode;
      String[] dstComponents = PathUtils.getPathComponents(dstPath.getPath());
      if (dstParentDirectory.getChild(dstComponents[dstComponents.length - 1]) != null) {
        return false;
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      long opTimeMs = System.currentTimeMillis();
      if (!renameInternal(fileId, dstPath, false, opTimeMs)) {
        return false;
      }

      writeJournalEntry(new RenameEntry(fileId, dstPath.getPath(), opTimeMs));
      flushJournal();

      LOG.debug("Renamed " + srcPath + " to " + dstPath);
      return true;
    }
  }

  /**
   * Implements renaming.
   *
   * @param fileId the file id of the rename source
   * @param dstPath the path to the rename destionation
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @return
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  boolean renameInternal(long fileId, TachyonURI dstPath, boolean replayed, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    Inode srcInode = mInodeTree.getInodeById(fileId);
    TachyonURI srcPath = mInodeTree.getPath(srcInode);
    LOG.debug("Renaming " + srcPath + " to " + dstPath);

    // If the source file is persisted, rename it in the UFS.
    FileInfo fileInfo = getFileInfoInternal(srcInode);
    if (!replayed && fileInfo.isPersisted) {
      String ufsSrcPath = mMountTable.resolve(srcPath).toString();
      String ufsDstPath = mMountTable.resolve(dstPath).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsSrcPath, MasterContext.getConf());
      String parentPath = new TachyonURI(ufsDstPath).getParent().toString();
      // TODO(jiri): The following can be removed once directory creation is persisted onto UFS.
      if (!ufs.exists(parentPath) && !ufs.mkdirs(parentPath, true)) {
        LOG.error("Failed to create " + parentPath);
        return false;
      }
      if (!ufs.rename(ufsSrcPath, ufsDstPath)) {
        LOG.error("Failed to rename " + ufsSrcPath + " to " + ufsDstPath);
        return false;
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
    MasterContext.getMasterSource().incFilesRenamed();
    propagatePersisted(srcInode, replayed);

    return true;
  }

  private void renameFromEntry(RenameEntry entry) {
    MasterContext.getMasterSource().incRenameOps();
    try {
      renameInternal(entry.mFileId, new TachyonURI(entry.mDstPath), true, entry.mOpTimeMs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the pin status for a file. If the file is a directory, the pin status will be set
   * recursively to all of its descendants. Called via RPC.
   *
   * @param fileId the file id to set the pin status for
   * @param pinned the pin status
   * @throws FileDoesNotExistException
   */
  public void setPinned(long fileId, boolean pinned) throws FileDoesNotExistException {
    // TODO(gene): metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      setPinnedInternal(fileId, pinned, opTimeMs);
      writeJournalEntry(new SetPinnedEntry(fileId, pinned, opTimeMs));
      flushJournal();
    }
  }

  private void setPinnedInternal(long fileId, boolean pinned, long opTimeMs)
      throws FileDoesNotExistException {
    // This function should only be called from within synchronized (mInodeTree) blocks.
    Inode inode = mInodeTree.getInodeById(fileId);
    mInodeTree.setPinned(inode, pinned, opTimeMs);
  }

  private void setPinnedFromEntry(SetPinnedEntry entry) {
    try {
      setPinnedInternal(entry.getId(), entry.getPinned(), entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }

  /**
   * Frees or evicts all of the blocks of the file from tachyon storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed. Called
   * via RPC.
   *
   * @param fileId the file to free
   * @param recursive if true, and the file is a directory, all descendants will be freed
   * @return true if the file was freed
   * @throws FileDoesNotExistException
   */
  public boolean free(long fileId, boolean recursive) throws FileDoesNotExistException {
    // TODO(gene): metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);

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
    }
    return true;
  }

  /**
   * Gets the path of a file with the given id. Called by the internal web ui.
   *
   * @param fileId The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist.
   */
  public TachyonURI getPath(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      return mInodeTree.getPath(mInodeTree.getInodeById(fileId));
    }
  }

  /**
   *
   * @return the set of inode ids which are pinned. Called via RPC.
   */
  public Set<Long> getPinIdList() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinIdSet();
    }
  }

  /**
   * @return the ufs address for this master.
   */
  public String getUfsAddress() {
    return MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
  }

  /**
   * @return the white list. Called by the internal web ui.
   */
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  // TODO(gene): The following methods are for lineage, which is not fully functional yet.
  public void createDependency() {
    // TODO(gene): Implement lineage.
  }

  public DependencyInfo getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException {
    Dependency dependency = mDependencyMap.getFromDependencyId(dependencyId);
    if (dependency == null) {
      throw new DependencyDoesNotExistException("No dependency with id " + dependencyId);
    }
    return dependency.generateClientDependencyInfo();
  }

  public void requestFilesInDependency(int dependencyId) {
    Dependency dependency = mDependencyMap.getFromDependencyId(dependencyId);
    if (dependency != null) {
      LOG.info("Request files in dependency " + dependency);
      if (dependency.hasLostFile()) {
        mDependencyMap.recomputeDependency(dependencyId);
      }
    } else {
      LOG.error("There is no dependency with id " + dependencyId);
    }
  }

  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory " + inode);
        return;
      }
      InodeFile iFile = (InodeFile) inode;
      if (mDependencyMap.addLostFile(fileId) == null) {
        LOG.error("There is no dependency info for " + iFile + " . No recovery on that");
      } else {
        LOG.info("Reported file loss. Tachyon will recompute it: " + iFile);
      }
    }
  }

  public List<Integer> getPriorityDependencyList() {
    return mDependencyMap.getPriorityDependencyList();
  }

  // TODO(jiri): Make it possible to load directories and not just individual files.
  public long loadFileInfoFromUfs(TachyonURI path, boolean recursive)
      throws BlockInfoException, FileAlreadyExistException, FileDoesNotExistException,
      InvalidPathException, SuspectedFileSizeException, TachyonException {
    TachyonURI ufsPath;
    synchronized (mInodeTree) {
      ufsPath = mMountTable.resolve(path);
    }
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath.toString(), MasterContext.getConf());
    try {
      if (!ufs.exists(ufsPath.getPath())) {
        throw new FileDoesNotExistException(ufsPath.getPath());
      }
      long ufsBlockSizeByte = ufs.getBlockSizeByte(ufsPath.toString());
      long fileSizeByte = ufs.getFileSize(ufsPath.toString());
      // Metadata loaded from UFS has no TTL set.
      long fileId = createFile(path, ufsBlockSizeByte, recursive, Constants.NO_TTL);
      persistFile(fileId, fileSizeByte);
      return fileId;
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw new TachyonException(e.getMessage());
    }
  }

  public boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath) throws FileAlreadyExistException,
      FileDoesNotExistException, InvalidPathException, IOException {
    synchronized (mInodeTree) {
      InodeTree.CreatePathResult createResult = mkdir(tachyonPath, false);
      if (mountInternal(tachyonPath, ufsPath)) {
        writeJournalEntry(new AddMountPointEntry(tachyonPath, ufsPath));
        flushJournal();
        return true;
      }
      // Cleanup created directories in case the mount operation failed.
      long opTimeMs = System.currentTimeMillis();
      deleteFileInternal(createResult.getCreated().get(0).getId(), true, false, opTimeMs);
    }
    return false;
  }

  void mountFromEntry(AddMountPointEntry entry) throws InvalidPathException {
    TachyonURI tachyonPath = entry.getTachyonPath();
    TachyonURI ufsPath = entry.getUfsPath();
    if (!mountInternal(tachyonPath, ufsPath)) {
      LOG.error("Failed to mount " + ufsPath + " at " + tachyonPath);
    }
  }

  boolean mountInternal(TachyonURI tachyonPath, TachyonURI ufsPath) throws InvalidPathException {
    return mMountTable.add(tachyonPath, ufsPath);
  }

  public boolean unmount(TachyonURI tachyonPath) throws FileDoesNotExistException,
      InvalidPathException, IOException {
    synchronized (mInodeTree) {
      if (unmountInternal(tachyonPath)) {
        Inode inode = mInodeTree.getInodeByPath(tachyonPath);
        // Use the internal delete API, setting {@code replayed} to false to prevent the delete
        // operations from being persisted in the UFS.
        long fileId = inode.getId();
        long opTimeMs = System.currentTimeMillis();
        deleteFileInternal(fileId, true /* recursive */, true /* replayed */, opTimeMs);
        writeJournalEntry(new DeleteFileEntry(fileId, true /* recursive */, opTimeMs));
        writeJournalEntry(new DeleteMountPointEntry(tachyonPath));
        flushJournal();
        return true;
      }
    }
    return false;
  }

  void unmountFromEntry(DeleteMountPointEntry entry) throws InvalidPathException {
    TachyonURI tachyonPath = entry.getTachyonPath();
    if (!unmountInternal(tachyonPath)) {
      LOG.error("Failed to unmount " + tachyonPath);
    }
  }

  boolean unmountInternal(TachyonURI tachyonPath) throws InvalidPathException {
    return mMountTable.delete(tachyonPath);
  }

  /**
   * MasterInodeTTL periodic check.
   */
  private final class MasterInodeTTLCheckExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat()  {
      synchronized (mInodeTree) {
        Set<TTLBucket> expiredBuckets = mTTLBuckets.getExpiredBuckets(System.currentTimeMillis());
        for (TTLBucket bucket : expiredBuckets) {
          for (InodeFile file : bucket.getFiles()) {
            if (!file.isDeleted()) {
              // file.isPinned() is deliberately not checked because ttl will have effect no matter
              // whether the file is pinned.
              try {
                deleteFile(file.getId(), false);
              } catch (FileDoesNotExistException e) {
                LOG.error("file does not exit " + file.toString());
              } catch (InvalidPathException e) {
                LOG.error("invalid path for ttl check " + file.toString());
              } catch (IOException e) {
                LOG.error("IO exception for ttl check" + file.toString());
              }
            }
          }
        }

        mTTLBuckets.removeBuckets(expiredBuckets);
      }
    }
  }
}
