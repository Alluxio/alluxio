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

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.String.format;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * An impl of WorkerLocationPolicy.
 *
 * Requirements:
 * 1. Multiple policies available
 * 2. A policy is stateless Strategy object, which can be a process-unique singleton
 * 3. Created with factory methods
 */
public class ConsistentHashPolicy implements WorkerLocationPolicy {
  // TODO(jiacheng): add a property key for this
  public static final int DEFAULT_VIRTUAL_NODES_NUMBER = 2000;
  private static final ConsistentHashProvider HASH_PROVIDER =
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

  public ConsistentHashPolicy() {
    this(DEFAULT_VIRTUAL_NODES_NUMBER);
  }

  /**
   * Constructs a new {@link ConsistentHashPolicy}.
   *
   * @param numVirtualNodes number of virtual nodes
   */
  public ConsistentHashPolicy(int numVirtualNodes) {
    mNumVirtualNodes = numVirtualNodes;
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(
      List<BlockWorkerInfo> blockWorkerInfos, String fileId, int count) {
    if (blockWorkerInfos.isEmpty()) {
      return ImmutableList.of();
    }
    HASH_PROVIDER.refresh(blockWorkerInfos, mNumVirtualNodes);
    return HASH_PROVIDER.getMultiple(fileId, count);
  }
}
