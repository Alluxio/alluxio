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

package alluxio.client.file.dora;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.wire.WorkerIdentity;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An implementation of WorkerLocationPolicy.
 * <p>
 * A policy where a file path is matched to worker(s) by a consistenct hashing algorithm.
 * The hash algorithm makes sure the same path maps to the same worker sequence.
 * On top of that, consistent hashing makes sure worker membership changes incur minimal
 * hash changes.
 */
public class ConsistentHashPolicy implements WorkerLocationPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashPolicy.class);

  private final ConsistentHashProvider mHashProvider =
      new ConsistentHashProvider(100, Constants.SECOND_MS);
  /**
   * This is the number of virtual nodes in the consistent hashing algorithm.
   * In a consistent hashing algorithm, on membership changes, some virtual nodes are
   * re-distributed instead of rebuilding the whole hash table.
   * This guarantees the hash table is changed only in a minimal.
   * In order to achieve that, the number of virtual nodes should be X times the physical nodes
   * in the cluster, where X is a balance between redistribution granularity and size.
   */
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link ConsistentHashPolicy}.
   *
   * @param conf the configuration used by the policy
   */
  public ConsistentHashPolicy(AlluxioConfiguration conf) {
    mNumVirtualNodes = conf.getInt(PropertyKey.USER_CONSISTENT_HASH_VIRTUAL_NODE_COUNT_PER_WORKER);
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException {
    if (blockWorkerInfos.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Not enough workers in the cluster %d workers in the cluster but %d required",
          blockWorkerInfos.size(), count));
    }
    List<WorkerIdentity> workerIdentities = blockWorkerInfos.stream()
        .map(BlockWorkerInfo::getIdentity)
        .collect(Collectors.toList());
    mHashProvider.refresh(workerIdentities, mNumVirtualNodes);
    List<WorkerIdentity> workers = mHashProvider.getMultiple(fileId, count);
    if (workers.size() != count) {
      throw new ResourceExhaustedException(String.format(
          "Found %d workers from the hash ring but %d required", workers.size(), count));
    }
    LOG.error("worker IDs returned by hash provider: {}", workers);
    LOG.error("worker infos given by callers: {}", blockWorkerInfos);
    ImmutableList.Builder<BlockWorkerInfo> builder = ImmutableList.builder();
    // todo(bowen): this is quadratic complexity. examine if it's worthwhile to replace
    //  with an indexed map if #workers is huge
    for (WorkerIdentity worker : workers) {
      for (BlockWorkerInfo info : blockWorkerInfos) {
        if (info.getIdentity().equals(worker)) {
          builder.add(info);
          break;
        }
      }
    }
    List<BlockWorkerInfo> infos = builder.build();
    LOG.error("worker infos to return: {}", infos);
    return infos;
  }
}
