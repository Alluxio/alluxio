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

import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerInfo;
import alluxio.worker.dora.ConsistentHashWorkerClusterView;
import alluxio.worker.dora.WorkerClusterView;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An impl of ConsistentHashPolicy.
 */
@SuppressWarnings("UnstableApiUsage") // Guava hash API is beta
@ThreadSafe
public class ConsistentHashPolicy implements WorkerLocationPolicy {
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
  private static final int MAX_ATTEMPTS = 100;
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link ConsistentHashPolicy}.
   *
   * @param numVirtualNodes number of virtual nodes
   */
  public ConsistentHashPolicy(int numVirtualNodes) {
    mNumVirtualNodes = numVirtualNodes;
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(WorkerClusterView workerClusterView,
                                                   String fileId,
                                                   int count) {
    if (workerClusterView.isEmpty()) {
      return ImmutableList.of();
    }
    if (workerClusterView instanceof ConsistentHashWorkerClusterView) {
      ConsistentHashWorkerClusterView v = (ConsistentHashWorkerClusterView) workerClusterView;
      NavigableMap<Integer, WorkerInfo> hashRing = v.getHashRing();
      return getMultiple(hashRing, fileId, count);
    } else {
      NavigableMap<Integer, WorkerInfo> hashRing =
          ConsistentHashWorkerClusterView.buildHashRing(workerClusterView, mNumVirtualNodes);
      return getMultiple(hashRing, fileId, count);
    }
  }

  static List<BlockWorkerInfo> getMultiple(NavigableMap<Integer, WorkerInfo> hashRing, String key,
      int count) {
    Set<BlockWorkerInfo> workers = new HashSet<>();
    int attempts = 0;
    while (workers.size() < count && attempts < MAX_ATTEMPTS) {
      attempts++;
      workers.add(get(hashRing, key, attempts));
    }
    return ImmutableList.copyOf(workers);
  }

  @VisibleForTesting
  static BlockWorkerInfo get(NavigableMap<Integer, WorkerInfo> hashRing, String key, int index) {
    Hasher hasher = HASH_FUNCTION.newHasher();
    int hashKey = hasher.putUnencodedChars(key).putInt(index).hash().asInt();
    Map.Entry<Integer, WorkerInfo> entry = hashRing.ceilingEntry(hashKey);
    final WorkerInfo workerInfo;
    if (entry != null) {
      workerInfo = entry.getValue();
    } else {
      Map.Entry<Integer, WorkerInfo> firstEntry = hashRing.firstEntry();
      if (firstEntry == null) {
        throw new IllegalStateException("Hash provider is empty");
      }
      workerInfo = firstEntry.getValue();
    }
    return new BlockWorkerInfo(workerInfo.getAddress(), workerInfo.getCapacityBytes(),
        workerInfo.getUsedBytes());
  }

  @Override
  public WorkerClusterView createView(Iterable<WorkerInfo> workers) {
    return new ConsistentHashWorkerClusterView(workers, mNumVirtualNodes);
  }
}
