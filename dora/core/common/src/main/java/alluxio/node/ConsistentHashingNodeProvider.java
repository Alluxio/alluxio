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

package alluxio.node;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.collections.Pair;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ConsistentHashingNodeProvider<T> implements NodeProvider {
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
  private static final long UPDATE_INTERVAL_MS = Constants.MINUTE_MS;
  private final Function<T, String> mIdentifierFunction;
  private final Function<Pair<T, T>, Boolean> mNodeUpdateFunction;
  private Collection<T> mLastNodes = ImmutableList.of();
  private NavigableMap<Integer, T> mActiveNodesByConsistentHashing;
  private final int mVirtualNodes;
  private volatile long mLastUpdatedTimestamp = 0L;
  private final AtomicBoolean mNeedUpdate = new AtomicBoolean(false);

  /**
   *
   * @param nodes              the nodes to select
   * @param numVirtualNodes    number of virtual nodes
   * @param identifierFunction the function to provide identifier
   * @param nodeUpdateFunction the function to check whether should update node
   * @return the instance of the {@link ConsistentHashingNodeProvider}
   * @param <T> the type of node
   */
  public static <T> ConsistentHashingNodeProvider create(List<T> nodes, int numVirtualNodes,
      Function<T, String> identifierFunction,
      Function<Pair<List<T>, List<T>>, Boolean> nodeUpdateFunction) {
    ConsistentHashingNodeProvider consistentHashingNodeProvider =
        new ConsistentHashingNodeProvider(numVirtualNodes, identifierFunction, nodeUpdateFunction);
    if (nodes != null && !nodes.isEmpty()) {
      consistentHashingNodeProvider.refresh(nodes);
    }
    return consistentHashingNodeProvider;
  }

  private ConsistentHashingNodeProvider(int virtualNodes, Function<T, String> identifierFunction,
      Function<Pair<T, T>, Boolean> nodeUpdateFunction) {
    mVirtualNodes = virtualNodes;
    mIdentifierFunction = identifierFunction;
    mNodeUpdateFunction = nodeUpdateFunction;
  }

  @Override
  public List<T> get(Object identifier, int count) {
    if (count > mVirtualNodes) {
      count = mVirtualNodes;
    }
    ImmutableList.Builder<T> nodes = ImmutableList.builder();
    Set<T> unique = new HashSet<>();
    int hashKey = HASH_FUNCTION.hashString(format("%s", identifier  ), UTF_8).asInt();
    Map.Entry<Integer, T> entry = mActiveNodesByConsistentHashing.ceilingEntry(hashKey);
    T candidate;
    SortedMap<Integer, T> nextEntries;
    if (entry != null) {
      candidate = entry.getValue();
      nextEntries = mActiveNodesByConsistentHashing.tailMap(entry.getKey(), false);
    }
    else {
      candidate = mActiveNodesByConsistentHashing.firstEntry().getValue();
      nextEntries = mActiveNodesByConsistentHashing.tailMap(
          mActiveNodesByConsistentHashing.firstKey(), false);
    }
    unique.add(candidate);
    nodes.add(candidate);
    while (unique.size() < count) {
      for (Map.Entry<Integer, T> next : nextEntries.entrySet()) {
        candidate = next.getValue();
        if (!unique.contains(candidate)) {
          unique.add(candidate);
          nodes.add(candidate);
          if (unique.size() == count) {
            break;
          }
        }
      }
      nextEntries = mActiveNodesByConsistentHashing;
    }
    return nodes.build();
  }

  @Override
  public void refresh(List nodes) {
    // check if we need to update worker info
    if (mLastUpdatedTimestamp <= 0L
        || System.currentTimeMillis() - mLastUpdatedTimestamp > UPDATE_INTERVAL_MS) {
      mNeedUpdate.set(true);
    }
    // update worker info if needed
    if (mNeedUpdate.compareAndSet(true, false)) {
      if (mNodeUpdateFunction.apply(new Pair(nodes, mLastNodes))) {
        build(nodes);
      }
      mLastUpdatedTimestamp = System.currentTimeMillis();
    }
  }

  private void build(Collection<T> nodes) {
    NavigableMap<Integer, T> activeNodesByConsistentHashing = new TreeMap<>();
    int weight = (int) ceil(1.0 * mVirtualNodes / nodes.size());
    for (T node : nodes) {
      for (int i = 0; i < weight; i++) {
        activeNodesByConsistentHashing.put(
            HASH_FUNCTION.hashString(format("%s%d", mIdentifierFunction.apply(node), i),
                UTF_8).asInt(),
            node);
      }
    }
    mLastNodes = nodes;
    mActiveNodesByConsistentHashing = activeNodesByConsistentHashing;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("VirtualNodes", mVirtualNodes)
        .add("mActiveNodesSize",
            mActiveNodesByConsistentHashing == null ? -1 : mActiveNodesByConsistentHashing.size())
        .toString();
  }
}
