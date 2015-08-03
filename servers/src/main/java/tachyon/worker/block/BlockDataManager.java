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

package tachyon.worker.block;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Users;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Class is responsible for managing the Tachyon BlockStore and Under FileSystem. This class is
 * thread-safe.
 */
public class BlockDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block store delta reporter for master heartbeat */
  private final BlockHeartbeatReporter mHeartbeatReporter;
  /** Block Store manager */
  private final BlockStore mBlockStore;
  /** Master client thread pool */
  private final ExecutorService mMasterClientExecutorService;
  /** Configuration values */
  private final TachyonConf mTachyonConf;
  /** WorkerSource for collecting worker metrics */
  private final WorkerSource mWorkerSource;
  /** Metrics reporter that listens on block events and increases metrics counters*/
  private final BlockMetricsReporter mMetricsReporter;

  // TODO: See if this can be removed from the class
  /** MasterClient, only used to inform the master of a new block in commitBlock */
  private MasterClient mMasterClient;
  /** UnderFileSystem Client */
  private UnderFileSystem mUfs;
  /** User metadata, used to keep track of user heartbeats */
  private Users mUsers;
  /** Id of this worker */
  private long mWorkerId;

  /**
   * Creates a BlockDataManager based on the configuration values.
   *
   * @param tachyonConf the configuration values to use
   * @param workerSource object for collecting the worker metrics
   * @throws IOException if the tiered store fails to initialize
   */
  public BlockDataManager(TachyonConf tachyonConf, WorkerSource workerSource) throws IOException {
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mBlockStore = new TieredBlockStore(tachyonConf);
    mTachyonConf = tachyonConf;
    mWorkerSource = workerSource;
    mMetricsReporter = new BlockMetricsReporter(mWorkerSource);

    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1,
            ThreadFactoryUtils.build("worker-client-heartbeat-%d", true));
    mMasterClient =
        new MasterClient(BlockWorkerUtils.getMasterAddress(mTachyonConf),
            mMasterClientExecutorService, mTachyonConf);

    // Create Under FileSystem Client
    String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
    String ufsAddress =
        mTachyonConf.get(Constants.UNDERFS_ADDRESS, tachyonHome + "/underFSStorage");
    mUfs = UnderFileSystem.get(ufsAddress, mTachyonConf);

    // Connect to UFS to handle UFS security
    InetSocketAddress workerAddress = BlockWorkerUtils.getWorkerAddress(mTachyonConf);
    mUfs.connectFromWorker(mTachyonConf, NetworkUtils.getFqdnHost(workerAddress));

    // Register the heartbeat reporter so it can record block store changes
    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
  }

  /**
   * Aborts the temporary block created by the user.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be aborted
   * @throws IOException if the block does not exist
   */
  public void abortBlock(long userId, long blockId) throws IOException {
    mBlockStore.abortBlock(userId, blockId);
  }

  /**
   * Access the block for a given user. This should be called to update the evictor when necessary.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to access
   * @throws IOException this exception is not thrown in the tiered block store implementation
   */
  public void accessBlock(long userId, long blockId) throws IOException {
    mBlockStore.accessBlock(userId, blockId);
  }

  /**
   * Add the checkpoint information of a file. The information is from the user <code>userId</code>.
   *
   * This method is normally triggered from {@link tachyon.client.FileOutStream#close()} if and only
   * if {@link tachyon.client.WriteType#isThrough()} is true. The current implementation of
   * checkpointing is that through {@link tachyon.client.WriteType} operations write to
   * {@link tachyon.underfs.UnderFileSystem} on the client's write path, but under a user temp
   * directory (temp directory is defined in the worker as {@link #getUserUfsTmpFolder(long)}).
   *
   * @param userId The user id of the client who sends the notification
   * @param fileId The id of the checkpointed file
   * @throws TException if the file does not exist or cannot be renamed
   * @throws IOException if the update to the master fails
   */
  public void addCheckpoint(long userId, int fileId) throws TException, IOException {
    // TODO This part needs to be changed.
    String srcPath = CommonUtils.concatPath(getUserUfsTmpFolder(userId), fileId);
    String ufsDataFolder =
        mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    String dstPath = CommonUtils.concatPath(ufsDataFolder, fileId);
    try {
      if (!mUfs.rename(srcPath, dstPath)) {
        throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
      }
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
    }
    long fileSize;
    try {
      fileSize = mUfs.getFileSize(dstPath);
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to getFileSize " + dstPath);
    }
    mMasterClient.addCheckpoint(mWorkerId, fileId, fileSize, dstPath);
  }

  /**
   * Cleans up after users, to prevent zombie users. This method is called periodically.
   */
  public void cleanupUsers() throws IOException {
    for (long user : mUsers.getTimedOutUsers()) {
      mUsers.removeUser(user);
      mBlockStore.cleanupUser(user);
    }
  }

  /**
   * Commits a block to Tachyon managed space. The block must be temporary. The block is
   * persisted after {@link BlockStore#commitBlock(long, long)}. The block will not be accessible
   * until {@link MasterClient#worker_cacheBlock(long, long, long, long, long)}
   * succeeds
   *
   * @param userId The id of the client
   * @param blockId The id of the block to commit
   * @return true if successful, false otherwise
   * @throws IOException if the block to commit does not exist
   */
  public void commitBlock(long userId, long blockId) throws IOException {
    mBlockStore.commitBlock(userId, blockId);

    // TODO: Reconsider how to do this without heavy locking
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(userId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(userId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      Long storageDirId = loc.getStorageDirId();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias() - 1);
      mMasterClient
          .worker_cacheBlock(mWorkerId, bytesUsedOnTier, storageDirId, blockId, length);
    } catch (TException te) {
      throw new IOException("Failed to commit block to master.", te);
    } finally {
      mBlockStore.unlockBlock(userId, blockId);
    }
  }

  /**
   * Creates a block in Tachyon managed space. The block will be temporary until it is committed.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to create
   * @param tierAlias The alias of the tier to place the new block in, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @return A string representing the path to the local file
   * @throws IOException if the block already exists
   * @throws OutOfSpaceException if there is no more space to store the block
   */
  // TODO: We should avoid throwing IOException
  public String createBlock(long userId, long blockId, int tierAlias, long initialBytes)
      throws IOException, OutOfSpaceException {
    BlockStoreLocation loc =
        tierAlias == -1 ? BlockStoreLocation.anyTier() : BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    return createdBlock.getPath();
  }

  /**
   * Creates a block. This method is only called from a data server.
   *
   * Call {@link #getTempBlockWriterRemote(long, long)} to get a writer for writing to the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be created
   * @param tierAlias The alias of the tier to place the new block in, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @throws FileDoesNotExistException if the block is not on the worker
   * @throws IOException if the block writer cannot be obtained
   */
  // TODO: We should avoid throwing IOException
  public void createBlockRemote(long userId, long blockId, int tierAlias, long initialBytes)
      throws FileDoesNotExistException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    CommonUtils.createBlockPath(createdBlock.getPath());
  }

  /**
   * Opens a {@link BlockWriter} for an existing temporary block. This method is only called from a
   * data server.
   *
   * The temporary block must already exist with {@link #createBlockRemote(long, long, int, long)}.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be opened for writing
   * @return the block writer for the local block file
   * @throws IOException if the block writer cannot be obtained
   */
  public BlockWriter getTempBlockWriterRemote(long userId, long blockId)
      throws FileDoesNotExistException, IOException {
    return mBlockStore.getBlockWriter(userId, blockId);
  }

  /**
   * Gets a report for the periodic heartbeat to master. Contains the blocks added since the last
   * heart beat and blocks removed since the last heartbeat.
   *
   * @return a block heartbeat report
   */
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  /**
   * Gets the metadata for the entire block store. Contains the block mapping per storage dir and
   * the total capacity and used capacity of each tier.
   *
   * @return the block store metadata
   */
  public BlockStoreMeta getStoreMeta() {
    return mBlockStore.getBlockStoreMeta();
  }

  /**
   * Gets the temporary folder for the user in the under filesystem.
   *
   * @param userId The id of the client
   * @return the path to the under filesystem temporary folder for the client
   */
  public String getUserUfsTmpFolder(long userId) {
    return mUsers.getUserUfsTempFolder(userId);
  }

  /**
   * Gets the metadata of a block given its blockId or throws IOException. This method does not
   * require a lock ID so the block is possible to be moved or removed after it returns.
   *
   * @param blockId the block ID
   * @return metadata of the block
   * @throws IOException if no BlockMeta for this blockId is found
   */
  public BlockMeta getVolatileBlockMeta(long blockId) throws IOException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block ID
   * @return true if the block is contained, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  /**
   * Obtains a read lock the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be locked
   * @return the lockId that uniquely identifies the lock obtained
   * @throws IOException if the lock cannot be obtained, for example because the block does not
   * exist
   */
  // TODO: We should avoid throwing IOException
  public long lockBlock(long userId, long blockId) throws IOException {
    return mBlockStore.lockBlock(userId, blockId);
  }

  /**
   * Moves a block from its current location to a target location, currently only tier level moves
   * are supported
   *
   * @param userId The id of the client
   * @param blockId The id of the block to move
   * @param tierAlias The tier to move the block to
   * @throws IOException if an error occurs during move
   */
  // TODO: We should avoid throwing IOException
  public void moveBlock(long userId, long blockId, int tierAlias) throws IOException {
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.moveBlock(userId, blockId, dst);
  }

  /**
   * Gets the path to the block file in local storage. The block must be a permanent block, and the
   * caller must first obtain the lock on the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return a string representing the path to this block in local storage
   * @throws IOException if the block cannot be found
   */
  public String readBlock(long userId, long blockId, long lockId) throws IOException {
    BlockMeta meta = mBlockStore.getBlockMeta(userId, blockId, lockId);
    return meta.getPath();
  }

  /**
   * Gets the block reader for the block. This method is only called by a data server.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return the block reader for the block
   * @throws IOException if an error occurs when obtaining the reader
   */
  // TODO: We should avoid throwing IOException
  public BlockReader readBlockRemote(long userId, long blockId, long lockId) throws IOException {
    return mBlockStore.getBlockReader(userId, blockId, lockId);
  }

  /**
   * Frees a block from Tachyon managed space.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be freed
   * @throws IOException if an error occurs when removing the block
   */
  // TODO: We should avoid throwing IOException
  public void removeBlock(long userId, long blockId) throws IOException {
    mBlockStore.removeBlock(userId, blockId);
  }

  /**
   * Request an amount of space for a block in its storage directory. The block must be a temporary
   * block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to allocate space to
   * @param additionalBytes The amount of bytes to allocate
   * @throws IOException if an error occurs when allocating space
   */
  // TODO: We should avoid throwing IOException
  public void requestSpace(long userId, long blockId, long additionalBytes) throws IOException {
    mBlockStore.requestSpace(userId, blockId, additionalBytes);
  }

  /**
   * Instantiates the user metadata object. This should only be called once and is a temporary work
   * around.
   *
   * @param users The user metadata object
   */
  public void setUsers(Users users) {
    mUsers = users;
  }

  /**
   * Sets the workerId. This should only be called once and is a temporary work around.
   *
   * @param workerId Worker id to update to
   */
  public void setWorkerId(long workerId) {
    mWorkerId = workerId;
  }

  /**
   * Stop the block data manager. This method should only be called when terminating the worker.
   */
  public void stop() {
    mMasterClient.close();
    mMasterClientExecutorService.shutdown();
  }

  /**
   * Relinquishes the lock with the specified lock id.
   *
   * @param lockId The id of the lock to relinquish
   */
  public void unlockBlock(long lockId) throws IOException {
    mBlockStore.unlockBlock(lockId);
  }

  // TODO: Remove when lock and reads are separate operations
  public void unlockBlock(long userId, long blockId) throws IOException {
    mBlockStore.unlockBlock(userId, blockId);
  }

  /**
   * Handles the heartbeat from a client.
   *
   * @param userId The id of the client
   * @param metrics The set of metrics the client has gathered since the last heartbeat
   */
  public void userHeartbeat(long userId, List<Long> metrics) {
    mUsers.userHeartbeat(userId);
    // TODO: Provide a way to disable the metrics collection
    if (null != metrics && !metrics.isEmpty() && metrics.get(Constants.CLIENT_METRICS_VERSION_INDEX)
        == Constants.CLIENT_METRICS_VERSION) {
      mWorkerSource.incBlocksReadLocal(metrics.get(Constants.BLOCKS_READ_LOCAL_INDEX));
      mWorkerSource.incBlocksReadRemote(metrics.get(Constants.BLOCKS_READ_REMOTE_INDEX));
      mWorkerSource.incBlocksWrittenLocal(metrics.get(Constants.BLOCKS_WRITTEN_LOCAL_INDEX));
      mWorkerSource.incBlocksWrittenRemote(metrics.get(Constants.BLOCKS_WRITTEN_REMOTE_INDEX));
      mWorkerSource.incBytesReadLocal(metrics.get(Constants.BYTES_READ_LOCAL_INDEX));
      mWorkerSource.incBytesReadRemote(metrics.get(Constants.BYTES_READ_REMOTE_INDEX));
      mWorkerSource.incBytesReadUfs(metrics.get(Constants.BYTES_READ_UFS_INDEX));
      mWorkerSource.incBytesWrittenLocal(metrics.get(Constants.BYTES_WRITTEN_LOCAL_INDEX));
      mWorkerSource.incBytesWrittenRemote(metrics.get(Constants.BYTES_WRITTEN_REMOTE_INDEX));
      mWorkerSource.incBytesWrittenUfs(metrics.get(Constants.BYTES_WRITTEN_UFS_INDEX));
    }
  }

  /**
   * Set the pinlist for the underlying blockstore.
   * Typically called by PinListSync.
   *
   * @param pinnedInodes, a set of pinned inodes
   */
  public void updatePinList(Set<Integer> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }
}
