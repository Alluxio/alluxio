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

package alluxio.client.block.policy;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * A policy based on the consistent hashing function to minimize the impact of scaling up and down.
 */
public class ConsistentHashPolicy implements BlockLocationPolicy {
  private static final ConsistentHashProvider HASH_PROVIDER = new ConsistentHashProvider();
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link ConsistentHashPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public ConsistentHashPolicy(AlluxioConfiguration conf) {
    mNumVirtualNodes =
        conf.getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_CONSISTENT_HASH_VIRTUAL_NODES);
  }

  @Nullable
  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    if (options.getBlockWorkerInfos().size() == 0) {
      return null;
    }
    HASH_PROVIDER.refresh(options.getBlockWorkerInfos(), mNumVirtualNodes);
    return HASH_PROVIDER.get(String.valueOf(options.getBlockInfo().getBlockId()), 0)
        .getNetAddress();
  }

  @Override
  public Set<BlockWorkerInfo> getWorkers(GetWorkerOptions options, int count) {
    if (options.getBlockWorkerInfos().size() == 0) {
      return Collections.EMPTY_SET;
    }
    HASH_PROVIDER.refresh(options.getBlockWorkerInfos(), mNumVirtualNodes);
    return HASH_PROVIDER.getMultiple(String.valueOf(options.getBlockInfo().getBlockId()), count);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConsistentHashPolicy)) {
      return false;
    }
    ConsistentHashPolicy that = (ConsistentHashPolicy) o;
    return Objects.equal(mNumVirtualNodes, that.mNumVirtualNodes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNumVirtualNodes);
  }

  private static class ConsistentHashProvider {
    private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
    private static final int MAX_ATTEMPTS = 100;
    private List<BlockWorkerInfo> mLastWorkerInfos;
    private NavigableMap<Integer, BlockWorkerInfo> mActiveNodesByConsistentHashing;

    public void refresh(List<BlockWorkerInfo> workerInfos, int numVirtualNodes) {
      if (workerInfos != mLastWorkerInfos
          && !ImmutableSet.copyOf(workerInfos).equals(ImmutableSet.copyOf(mLastWorkerInfos))) {
        build(workerInfos, numVirtualNodes);
      }
    }

    public Set<BlockWorkerInfo> getMultiple(String key, int count) {
      Set<BlockWorkerInfo> workers = new HashSet<>();
      int attempts = 0;
      while (workers.size() < count && attempts < MAX_ATTEMPTS) {
        attempts++;
        workers.add(get(key, attempts));
      }
      return Collections.unmodifiableSet(workers);
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
