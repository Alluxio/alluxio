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
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.client.block.BlockWorkerInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * An impl of WorkerLocationPolicy.
 */
public class WorkerLocationPolicy {
  private static final ConsistentHashProvider HASH_PROVIDER = new ConsistentHashProvider();
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link WorkerLocationPolicy}.
   *
   * @param numVirtualNodes number of virtual nodes
   */
  public WorkerLocationPolicy(int numVirtualNodes) {
    mNumVirtualNodes = numVirtualNodes;
  }

  /**
   *
   * @param blockWorkerInfos
   * @param fileId
   * @param count
   * @return a list of preferred workers
   */
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId,
      int count) {
    if (blockWorkerInfos.size() == 0) {
      return ImmutableList.of();
    }
    HASH_PROVIDER.refresh(blockWorkerInfos, mNumVirtualNodes);
    return HASH_PROVIDER.getMultiple(fileId, count);
  }

  private static class ConsistentHashProvider {
    private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
    private static final int MAX_ATTEMPTS = 100;
    private List<BlockWorkerInfo> mLastWorkerInfos = ImmutableList.of();
    private NavigableMap<Integer, BlockWorkerInfo> mActiveNodesByConsistentHashing;

    public void refresh(List<BlockWorkerInfo> workerInfos, int numVirtualNodes) {
      if (workerInfos != mLastWorkerInfos
          && !ImmutableSet.copyOf(workerInfos).equals(ImmutableSet.copyOf(mLastWorkerInfos))) {
        build(workerInfos, numVirtualNodes);
      }
    }

    public List<BlockWorkerInfo> getMultiple(String key, int count) {
      Set<BlockWorkerInfo> workers = new HashSet<>();
      int attempts = 0;
      while (workers.size() < count && attempts < MAX_ATTEMPTS) {
        attempts++;
        workers.add(get(key, attempts));
      }
      return ImmutableList.copyOf(workers);
    }

    public BlockWorkerInfo get(String key, int index) {
      int hashKey = HASH_FUNCTION.hashString(format("%s%d", key, index), UTF_8).asInt();
      Map.Entry<Integer, BlockWorkerInfo> entry =
          mActiveNodesByConsistentHashing.ceilingEntry(hashKey);
      if (entry != null) {
        return mActiveNodesByConsistentHashing.ceilingEntry(hashKey).getValue();
      } else {
        return mActiveNodesByConsistentHashing.firstEntry().getValue();
      }
    }

    private void build(List<BlockWorkerInfo> workerInfos, int numVirtualNodes) {
      NavigableMap<Integer, BlockWorkerInfo> activeNodesByConsistentHashing = new TreeMap<>();
      int weight = (int) ceil(1.0 * numVirtualNodes / workerInfos.size());
      for (BlockWorkerInfo workerInfo : workerInfos) {
        for (int i = 0; i < weight; i++) {
          activeNodesByConsistentHashing.put(
              HASH_FUNCTION.hashString(format("%s%d", workerInfo.getNetAddress().toString(), i),
                  UTF_8).asInt(),
              workerInfo);
        }
      }
      mLastWorkerInfos = workerInfos;
      mActiveNodesByConsistentHashing = activeNodesByConsistentHashing;
    }
  }
}
