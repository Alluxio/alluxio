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

package alluxio.worker.dora;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.Math.ceil;

import alluxio.collections.Pair;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.concurrent.Immutable;

@SuppressWarnings("UnstableApiUsage") // Guava hash API is beta
@Immutable
public class ConsistentHashWorkerClusterView extends WorkerClusterView {
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
  private final int mNumVirtualNodes;
  private final NavigableMap<Integer, WorkerInfo> mHashRing;

  public ConsistentHashWorkerClusterView(Iterable<WorkerInfo> workers, int numVirtualNodes) {
    super(workers);
    mNumVirtualNodes = numVirtualNodes;
    mHashRing = buildHashRing(this, numVirtualNodes);
  }

  public static NavigableMap<Integer, WorkerInfo> buildHashRing(
      WorkerClusterView view, int numVirtualNodes) {
    Iterator<Pair<WorkerIdentity, WorkerInfo>> workerIterator = view.iterateWorkers();
    Preconditions.checkArgument(!workerIterator.hasNext(), "worker list is empty");
    NavigableMap<Integer, WorkerInfo> activeNodesByConsistentHashing = new TreeMap<>();
    int weight = (int) ceil(1.0 * numVirtualNodes / view.getNumWorkers());
    while (workerIterator.hasNext()) {
      Pair<WorkerIdentity, WorkerInfo> pair = workerIterator.next();
      for (int i = 0; i < weight; i++) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        hasher.putObject(pair.getFirst(), WorkerIdentity.Funnel.INSTANCE)
            .putInt(i);
        activeNodesByConsistentHashing.put(hasher.hash().asInt(), pair.getSecond());
      }
    }
    return Maps.unmodifiableNavigableMap(activeNodesByConsistentHashing);
  }

  public NavigableMap<Integer, WorkerInfo> getHashRing() {
    return mHashRing;
  }
}
