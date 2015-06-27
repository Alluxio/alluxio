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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Optional;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Users;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Class responsible for managing the Tachyon BlockStore and Under FileSystem. This class is
 * thread-safe.
 */
public class BlockDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block store delta reporter for master heartbeat */
  private final BlockHeartbeatReporter mHeartbeatReporter;
  /** Block Store manager */
  private final BlockStore mBlockStore;
  /** Master client threadpool */
  private final ExecutorService mMasterClientExecutorService;
  /** Configuration values */
  private final TachyonConf mTachyonConf;

  // TODO: See if this can be removed from the class
  /** MasterClient, only used to inform the master of a new block in commitBlock */
  private MasterClient mMasterClient;
  /** User metadata, used to keep track of user heartbeats */
  private Users mUsers;
  /** Id of this worker */
  long mWorkerId;

  /**
   * Creates a BlockDataManager based on the configuration values.
   *
   * @param tachyonConf the configuration values to use
   */
  public BlockDataManager(TachyonConf tachyonConf) {
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mBlockStore = new TieredBlockStore(tachyonConf);
    mTachyonConf = tachyonConf;
    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-client-heartbeat-%d"));
    mMasterClient =
        new MasterClient(getMasterAddress(), mMasterClientExecutorService, mTachyonConf);

    // Register the heartbeat reporter so it can record block store changes
    mBlockStore.registerMetaListener(mHeartbeatReporter);
  }

  /**
   * Aborts the temporary block created by the user.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be aborted
   * @return true if successful, false if unsuccessful
   * @throws IOException if the block does not exist
   */
  // TODO: This may be better as void
  public boolean abortBlock(long userId, long blockId) throws IOException {
    return mBlockStore.abortBlock(userId, blockId);
  }

  /**
   * Access the block for a given user. This should be called to update the evictor when necessary.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to access
   */
  public void accessBlock(long userId, long blockId) {
    mBlockStore.accessBlock(userId, blockId);
  }

  /**
   * Cleans up after users, to prevent zombie users. This method is called periodically.
   */
  public void cleanupUsers() {
    for (long user : mUsers.getTimedOutUsers()) {
      mUsers.removeUser(user);
      mBlockStore.cleanupUser(user);
    }
  }

  /**
   * Commits a block to Tachyon managed space. The block must be temporary.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to commit
   * @return true if successful, false otherwise
   * @throws IOException if the block to commit does not exist
   */
  // TODO: This may be better as void
  public boolean commitBlock(long userId, long blockId) throws IOException {
    if (!mBlockStore.commitBlock(userId, blockId)) {
      return false;
    }

    // Block successfully committed, update master with new block metadata
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId);
    if (!optLock.isPresent()) {
      throw new IOException("Error while locking new block: " + blockId);
    }
    Long lockId = optLock.get();
    Optional<BlockMeta> optMeta = mBlockStore.getBlockMeta(userId, blockId, lockId);
    if (!optMeta.isPresent()) {
      throw new IOException("Failed to get block meta for new block " + blockId);
    }
    BlockMeta meta = optMeta.get();
    BlockStoreLocation loc = meta.getBlockLocation();
    Long storageDirId = loc.getStorageDirId();
    Long length = meta.getBlockSize();
    BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
    Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierLevel());
    try {
      mMasterClient
          .worker_cacheBlock(mWorkerId, bytesUsedOnTier, storageDirId, blockId, length);
    } catch (TException te) {
      throw new IOException("Failed to commit block to master.", te);
    }
    return true;
  }

  /**
   * Creates a block in Tachyon managed space. The block will be temporary until it is committed.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to create
   * @param location The tier to place the new block in, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @return A string representing the path to the local file
   * @throws IOException if the block already exists
   * @throws OutOfSpaceException if there is no more space to store the block
   */
  // TODO: We should avoid throwing IOException
  public String createBlock(long userId, long blockId, int location, long initialBytes)
      throws IOException, OutOfSpaceException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(location);
    Optional<TempBlockMeta> optBlock =
        mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      return optBlock.get().getPath();
    }
    // Failed to allocate initial bytes
    throw new OutOfSpaceException("Failed to allocate " + initialBytes + " for user " + userId);
  }

  /**
   * Creates a block. This method is only called from a data server.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be created
   * @param location The tier to create this block, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @return the block writer for the local block file
   * @throws FileDoesNotExistException if the block is not on the worker
   * @throws IOException if the block writer cannot be obtained
   */
  // TODO: We should avoid throwing IOException
  public BlockWriter createBlockRemote(long userId, long blockId, int location, long initialBytes)
      throws FileDoesNotExistException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(location);
    Optional<TempBlockMeta> optBlock =
        mBlockStore.createBlockMeta(userId, blockId, loc, initialBytes);
    if (optBlock.isPresent()) {
      Optional<BlockWriter> optWriter = mBlockStore.getBlockWriter(userId, blockId);
      if (optWriter.isPresent()) {
        return optWriter.get();
      }
      throw new IOException("Failed to obtain block writer.");
    }
    throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
  }

  /**
   * Frees a block from Tachyon managed space.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be freed
   * @return true if successful, false otherwise
   * @throws IOException if an error occurs when removing the block
   */
  // TODO: This may be better as void, we should avoid throwing IOException
<<<<<<< HEAD
  public boolean removeBlock(long userId, long blockId) throws IOException {
    return mBlockStore.removeBlock(userId, blockId);
=======
  public boolean freeBlock(long userId, long blockId) throws IOException {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLockType.BlockLockType.WRITE);
    if (!optLock.isPresent()) {
      return false;
    }
    Long lockId = optLock.get();
    mBlockStore.removeBlock(userId, blockId, lockId);
    mBlockStore.unlockBlock(lockId);
    return true;
>>>>>>> Make lockBlock in BlockStore interface to grab READ lock only, because only all WRITE lock acquisition are in the tierd store layer
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
   * Obtains a read lock the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to be locked
   * @return the lockId, or -1 if we failed to obtain a lock
   */
  public long lockBlock(long userId, long blockId) {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId);
    if (optLock.isPresent()) {
      return optLock.get();
    }
    return -1;
  }

  /**
   * Moves a block from its current location to a target location, currently only tier level moves
   * are supported
   *
   * @param userId The id of the client
   * @param blockId The id of the block to move
   * @param tier The tier to move the block to
   * @return true if successful, false otherwise
   * @throws IOException if an error occurs during move
   */
  // TODO: This may be better as void, we should avoid throwing IOException
  public boolean moveBlock(long userId, long blockId, int tier) throws IOException {
    Optional<Long> optLock = mBlockStore.lockBlock(userId, blockId, BlockLockType.BlockLockType.WRITE);
    // TODO: Define this behavior
    if (!optLock.isPresent()) {
      return false;
    }
    Long lockId = optLock.get();
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tier);
    return mBlockStore.moveBlock(userId, blockId, dst);
  }

  /**
   * Gets the path to the block file in local storage. The block must be a permanent block, and the
   * caller must first obtain the lock on the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return a string representing the path to this block in local storage
   * @throws FileDoesNotExistException if the block does not exist
   */
  public String readBlock(long userId, long blockId, long lockId) throws FileDoesNotExistException {
    Optional<BlockMeta> optBlock = mBlockStore.getBlockMeta(userId, blockId, lockId);
    if (optBlock.isPresent()) {
      return optBlock.get().getPath();
    }
    // Failed to find the block
    throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
  }

  /**
   * Gets the block reader for the block. This method is only called by a data server.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return the block reader for the block
   * @throws FileDoesNotExistException if the block does not exist
   * @throws IOException if an error occurs when obtaining the reader
   */
  // TODO: We should avoid throwing IOException
  public BlockReader readBlockRemote(long userId, long blockId, long lockId) throws IOException {
    Optional<BlockReader> optReader = mBlockStore.getBlockReader(userId, blockId, lockId);
    if (optReader.isPresent()) {
      return optReader.get();
    }
    throw new IOException("Block " + blockId + " does not exist on this worker.");
  }

  /**
   * Request an amount of space for a block in its storage directory. The block must be a temporary
   * block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to allocate space to
   * @param bytesRequested The amount of bytes to allocate
   * @return true if the space was allocated, false otherwise
   * @throws IOException if an error occurs when allocating space
   */
  // TODO: This may be better as void, we should avoid throwing IOException
  public boolean requestSpace(long userId, long blockId, long bytesRequested) throws IOException {
    return mBlockStore.requestSpace(userId, blockId, bytesRequested);
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
   */
  public void setWorkerId(long workerId) {
    mWorkerId = workerId;
  }

  /**
   * Relinquishes the lock with the specified lock id.
   *
   * @param lockId The id of the lock to relinquish
   * @return true if successful, false otherwise
   */
  // TODO: This may be better as void
  public boolean unlockBlock(long lockId) {
    return mBlockStore.unlockBlock(lockId);
  }

  /**
   * Handles the heartbeat from a client.
   *
   * @param userId The id of the client
   * @param metrics The set of metrics the client has gathered since the last heartbeat
   * @return true if successful, false otherwise
   */
  // TODO: This may be better as void
  public boolean userHeartbeat(long userId, List<Long> metrics) {
    mUsers.userHeartbeat(userId);
    return true;
  }

  /**
   * Gets the Tachyon master address from the configuration
   *
   * @return the InetSocketAddress of the master
   */
  private InetSocketAddress getMasterAddress() {
    String masterHostname =
        mTachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(mTachyonConf));
    int masterPort = mTachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }
}
