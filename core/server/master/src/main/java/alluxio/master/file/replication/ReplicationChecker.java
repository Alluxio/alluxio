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
import alluxio.Constants;
import alluxio.client.job.JobMasterClientPool;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.plan.replicate.DefaultReplicationHandler;
import alluxio.job.plan.replicate.ReplicationHandler;
import alluxio.job.wire.Status;
import alluxio.master.SafeModeManager;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.logging.SamplingLogger;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The executor to check block replication level periodically and handle over-replicated and
 * under-replicated blocks correspondingly.
 */
@ThreadSafe
public final class ReplicationChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationChecker.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);

  /** Maximum number of active jobs to be submitted to the job service. **/
  private final int mMaxActiveJobs;

  /** Handler to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handler to the block master. */
  private final BlockMaster mBlockMaster;
  /** Handler for adjusting block replication level. */
  private final ReplicationHandler mReplicationHandler;
  /** Manager of master safe mode state. */
  private final SafeModeManager mSafeModeManager;

  private final HashBiMap<Long, Long> mActiveJobToInodeID;

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

    // Do not use more than 10% of the job service
    mMaxActiveJobs = Math.max(1,
        (int) (ServerConfiguration.getInt(PropertyKey.JOB_MASTER_JOB_CAPACITY) * 0.1));
    mActiveJobToInodeID = HashBiMap.create();
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
    final Set<Long> activeJobIds = new HashSet<>();
    try {
      if (!mActiveJobToInodeID.isEmpty()) {
        final List<Long> activeEvictJobIds =
            mReplicationHandler.findJobs("Evict",
                ImmutableSet.of(Status.RUNNING, Status.CREATED));
        final List<Long> activeMoveJobIds =
            mReplicationHandler.findJobs("Move",
                ImmutableSet.of(Status.RUNNING, Status.CREATED));
        final List<Long> activeReplicateJobIds =
            mReplicationHandler.findJobs("Replicate",
                ImmutableSet.of(Status.RUNNING, Status.CREATED));

        activeJobIds.addAll(activeEvictJobIds);
        activeJobIds.addAll(activeMoveJobIds);
        activeJobIds.addAll(activeReplicateJobIds);
        mActiveJobToInodeID.keySet().removeIf(jobId -> !activeJobIds.contains(jobId));
      }
    } catch (IOException e) {
      // It is possible the job master process is not answering rpcs,
      // log but do not throw the exception
      // which will kill the replication checker thread.
      LOG.debug("Failed to contact job master to get updated list of replication jobs {}", e);
    }

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
    if (pinnedMediumTypes.isEmpty()) {
      // nothing needs to be moved
      return Collections.emptyMap();
    }
    Map<String, String> movement = new HashMap<>();
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

  private void checkMisreplicated(Set<Long> inodes, ReplicationHandler handler)
      throws InterruptedException {
    for (long inodeId : inodes) {
      if (mActiveJobToInodeID.size() >= mMaxActiveJobs) {
        return;
      }
      if (mActiveJobToInodeID.containsValue(inodeId)) {
        continue;
      }
      // Throw if interrupted.
      if (Thread.interrupted()) {
        throw new InterruptedException("ReplicationChecker interrupted.");
      }
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
            LOG.warn("The block master is not available: {}", e.toString());
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
              final long jobId =
                  handler.migrate(inodePath.getUri(), blockId, entry.getKey(), entry.getValue());
              mActiveJobToInodeID.put(jobId, inodeId);
            } catch (Exception e) {
              LOG.warn(
                  "Unexpected exception encountered when starting a migration job (uri={},"
                      + " block ID={}, workerHost= {}) : {}",
                  inodePath.getUri(), blockId, entry.getKey(), e.toString());
              LOG.debug("Exception: ", e);
            }
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check replication level for inode id {} : {}", inodeId, e.toString());
      }
    }
  }

  private Set<Long> check(Set<Long> inodes, ReplicationHandler handler, Mode mode)
      throws InterruptedException {
    Set<Long> processedFileIds = new HashSet<>();
    for (long inodeId : inodes) {
      if (mActiveJobToInodeID.size() >= mMaxActiveJobs) {
        return processedFileIds;
      }
      if (mActiveJobToInodeID.containsValue(inodeId)) {
        continue;
      }
      Set<Triple<AlluxioURI, Long, Integer>> requests = new HashSet<>();
      // Throw if interrupted.
      if (Thread.interrupted()) {
        throw new InterruptedException("ReplicationChecker interrupted.");
      }
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
            LOG.warn("The block master is not available: {}", e.toString());
            return processedFileIds;
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
                if (!file.isPersisted() && mBlockMaster.isBlockLost(blockId)) {
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
        LOG.warn("Failed to check replication level for inode id {} : {}", inodeId, e.toString());
      }

      for (Triple<AlluxioURI, Long, Integer> entry : requests) {
        AlluxioURI uri = entry.getLeft();
        long blockId = entry.getMiddle();
        int numReplicas = entry.getRight();
        try {
          long jobId;
          switch (mode) {
            case EVICT:
              jobId = handler.evict(uri, blockId, numReplicas);
              break;
            case REPLICATE:
              jobId = handler.replicate(uri, blockId, numReplicas);
              break;
            default:
              throw new RuntimeException(String.format("Unexpected replication mode {}.", mode));
          }
          processedFileIds.add(inodeId);
          mActiveJobToInodeID.put(jobId, inodeId);
        } catch (JobDoesNotExistException | ResourceExhaustedException e) {
          LOG.warn("The job service is busy, will retry later. {}", e.toString());
          return processedFileIds;
        } catch (UnavailableException e) {
          LOG.warn("Unable to complete the replication check: {}, will retry later.", e.toString());
          return processedFileIds;
        } catch (Exception e) {
          SAMPLING_LOG.warn(
              "Unexpected exception encountered when starting a {} job (uri={},"
                  + " block ID={}, num replicas={}) : {}",
              mode, uri, blockId, numReplicas, e.toString());
          LOG.debug("Job service unexpected exception: ", e);
        }
      }
    }
    return processedFileIds;
  }
}
