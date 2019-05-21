/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.replication;

import alluxio.AlluxioURI;
import alluxio.client.job.JobMasterClientPool;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.replicate.DefaultReplicationHandler;
import alluxio.job.replicate.ReplicationHandler;
import alluxio.master.SafeModeManager;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.PersistenceState;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The executor to check block replication level periodically and handle over-replicated and
 * under-replicated blocks correspondingly.
 */
@ThreadSafe
public final class ReplicationChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationChecker.class);
  private static final long MAX_QUIET_PERIOD_SECONDS = 64;

  /** Handler to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handler to the block master. */
  private final BlockMaster mBlockMaster;
  /** Handler for adjusting block replication level. */
  private final ReplicationHandler mReplicationHandler;
  /** Manager of master safe mode state. */
  private final SafeModeManager mSafeModeManager;

  /**
   * Quiet period for job service flow control (in seconds). When job service refuses starting new
   * jobs, we use exponential backoff to alleviate the job service pressure.
   */
  private long mQuietPeriodSeconds;

  private enum Mode {
    EVICT,
    REPLICATE
  }

  /**
   * Constructs a new {@link ReplicationChecker} using default (job service) handler to replicate
   * and evict blocks.
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   * @param safeModeManager manager of master safe mode state
   * @param jobMasterClientPool job master client pool
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster,
      SafeModeManager safeModeManager, JobMasterClientPool jobMasterClientPool) {
    this(inodeTree, blockMaster, safeModeManager,
        new DefaultReplicationHandler(jobMasterClientPool));
  }

  /**
   * Constructs a new {@link ReplicationChecker} with specified replicate and evict handlers (for
   * unit testing).
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   * @param safeModeManager manager of master safe mode state
   * @param replicationHandler handler to replicate blocks
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster,
      SafeModeManager safeModeManager, ReplicationHandler replicationHandler) {
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
    mSafeModeManager = safeModeManager;
    mReplicationHandler = replicationHandler;
    mQuietPeriodSeconds = 0;
  }

  /**
   * {@inheritDoc}
   *
   * During this heartbeat, this class will check:
   * <p>
   * (1) Is there any block from the pinned files becomes under replicated (i.e., the number of
   * existing copies is smaller than the target replication min for this file, possibly due to node
   * failures), and schedule replicate jobs to increase the replication level when found;
   *
   * (2) Is there any blocks over replicated, schedule evict jobs to reduce the replication level.
   */
  @Override
  public void heartbeat() throws InterruptedException {
    // skips replication in safe mode when not all workers are registered
    if (mSafeModeManager.isInSafeMode()) {
      return;
    }

    TimeUnit.SECONDS.sleep(mQuietPeriodSeconds);
    Set<Long> inodes;

    // Check the set of files that could possibly be under-replicated
    inodes = mInodeTree.getPinIdSet();
    check(inodes, mReplicationHandler, Mode.REPLICATE);

    // Check the set of files that could possibly be over-replicated
    inodes = mInodeTree.getReplicationLimitedFileIds();
    check(inodes, mReplicationHandler, Mode.EVICT);

    // Check the set of files that could possibly be mis-replicated
    inodes = mInodeTree.getPinIdSet();
    checkMisreplicated(inodes, mReplicationHandler);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  /**
   * Find a set of from and to locations for our moveBlock operation.
   * After the move, the file will be in the desired pinned medium at least minReplication times.
   *
   * @param file the file to scan for misplaced blocks
   * @param blockInfo blockInfo containing information about the block locations
   * @return a map that maps from workerHost to desired medium
   */
  private Map<String, String> findMisplacedBlock(
      InodeFile file, BlockInfo blockInfo) {
    Set<String> pinnedMediumTypes = file.getMediumTypes();
    Map<String, String> movement = new HashMap<>();
    if (pinnedMediumTypes.isEmpty()) {
      // nothing needs to be moved
      return Collections.emptyMap();
    }
    // at least pinned to one medium type
    String firstPinnedMedium = pinnedMediumTypes.iterator().next();
    int minReplication = file.getReplicationMin();
    int correctReplication = 0;
    List<String> candidates = new ArrayList<>();
    for (BlockLocation loc: blockInfo.getLocations()) {
      if (pinnedMediumTypes.contains(loc.getMediumType())) {
        correctReplication++;
      } else {
        candidates.add(loc.getWorkerAddress().getHost());
      }
    }
    if (correctReplication >= minReplication) {
      // there are more than minReplication in the right location
      return Collections.emptyMap();
    } else {
      int toMove = minReplication - correctReplication;
      for (String candidate : candidates) {
        // We are only addressing pinning to a single medium case, in the future we might
        // address pinning to multiple medium types.
        movement.put(candidate, firstPinnedMedium);
        toMove--;
        if (toMove == 0) {
          return movement;
        }
      }
    }
    return movement;
  }

  private void checkMisreplicated(Set<Long> inodes, ReplicationHandler handler) {
    for (long inodeId : inodes) {
      try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(inodeId, LockPattern.READ)) {
        InodeFile file = inodePath.getInodeFile();
        for (long blockId : file.getBlockIds()) {
          BlockInfo blockInfo = null;
          try {
            blockInfo = mBlockMaster.getBlockInfo(blockId);
          } catch (BlockInfoException e) {
            // Cannot find this block in Alluxio from BlockMaster, possibly persisted in UFS
          } catch (UnavailableException e) {
            // The block master is not available, wait for the next heartbeat
            LOG.warn("The block master is not available: {}", e.getMessage());
            return;
          }
          if (blockInfo == null) {
            // no block info available, we simply log and return;
            LOG.warn("Block info is null");
            return;
          }

          for (Map.Entry<String, String> entry
              : findMisplacedBlock(file, blockInfo).entrySet()) {
            try {
              handler.migrate(inodePath.getUri(), blockId, entry.getKey(), entry.getValue());
            } catch (Exception e) {
              LOG.warn(
                  "Unexpected exception encountered when starting a migration job (uri={},"
                      + " block ID={}, workerHost= {}) : {}",
                  inodePath.getUri(), blockId, entry.getKey(), e.getMessage());
              LOG.debug("Exception: ", e);
            }
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check replication level for inode id {} : {}", inodeId, e.getMessage());
      }
    }
  }

  private void check(Set<Long> inodes, ReplicationHandler handler, Mode mode) {
    Set<Long> lostBlocks = mBlockMaster.getLostBlocks();
    Set<Triple<AlluxioURI, Long, Integer>> requests = new HashSet<>();
    for (long inodeId : inodes) {
      // TODO(binfan): calling lockFullInodePath locks the entire path from root to the target
      // file and may increase lock contention in this tree. Investigate if we could avoid
      // locking the entire path but just the inode file since this access is read-only.
      try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(inodeId, LockPattern.READ)) {
        InodeFile file = inodePath.getInodeFile();
        for (long blockId : file.getBlockIds()) {
          BlockInfo blockInfo = null;
          try {
            blockInfo = mBlockMaster.getBlockInfo(blockId);
          } catch (BlockInfoException e) {
            // Cannot find this block in Alluxio from BlockMaster, possibly persisted in UFS
          } catch (UnavailableException e) {
            // The block master is not available, wait for the next heartbeat
            LOG.warn("The block master is not available: {}", e.getMessage());
            return;
          }
          int currentReplicas = (blockInfo == null) ? 0 : blockInfo.getLocations().size();
          switch (mode) {
            case EVICT:
              int maxReplicas = file.getReplicationMax();
              if (file.getPersistenceState() == PersistenceState.TO_BE_PERSISTED
                  && file.getReplicationDurable() > maxReplicas) {
                maxReplicas = file.getReplicationDurable();
              }
              if (currentReplicas > maxReplicas) {
                requests.add(new ImmutableTriple<>(inodePath.getUri(), blockId,
                    currentReplicas - maxReplicas));
              }
              break;
            case REPLICATE:
              int minReplicas = file.getReplicationMin();
              if (file.getPersistenceState() == PersistenceState.TO_BE_PERSISTED
                  && file.getReplicationDurable() > minReplicas) {
                minReplicas = file.getReplicationDurable();
              }
              if (currentReplicas < minReplicas) {
                // if this file is not persisted and block master thinks it is lost, no effort made
                if (!file.isPersisted() && lostBlocks.contains(blockId)) {
                  continue;
                }
                requests.add(new ImmutableTriple<>(inodePath.getUri(), blockId,
                    minReplicas - currentReplicas));
              }
              break;
            default:
              LOG.warn("Unexpected replication mode {}.", mode);
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check replication level for inode id {} : {}", inodeId, e.getMessage());
      }
    }
    for (Triple<AlluxioURI, Long, Integer> entry : requests) {
      AlluxioURI uri = entry.getLeft();
      long blockId = entry.getMiddle();
      int numReplicas = entry.getRight();
      try {
        switch (mode) {
          case EVICT:
            handler.evict(uri, blockId, numReplicas);
            mQuietPeriodSeconds /= 2;
            break;
          case REPLICATE:
            handler.replicate(uri, blockId, numReplicas);
            mQuietPeriodSeconds /= 2;
            break;
          default:
            LOG.warn("Unexpected replication mode {}.", mode);
        }
      } catch (JobDoesNotExistException | ResourceExhaustedException e) {
        LOG.warn("The job service is busy, will retry later. {}", e.toString());
        mQuietPeriodSeconds = (mQuietPeriodSeconds == 0) ? 1 :
            Math.min(MAX_QUIET_PERIOD_SECONDS, mQuietPeriodSeconds * 2);
        return;
      } catch (UnavailableException e) {
        LOG.warn("Unable to complete the replication check: {}, will retry later.",
            e.getMessage());
        return;
      } catch (Exception e) {
        LOG.warn(
            "Unexpected exception encountered when starting a replication / eviction job (uri={},"
                + " block ID={}, num replicas={}) : {}",
            uri, blockId, numReplicas, e.getMessage());
        LOG.debug("Exception: ", e);
      }
    }
  }
}
