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

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.collections.Pair;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class WorkerClusterView {
  protected static final IndexDefinition<Pair<WorkerIdentity, WorkerInfo>, WorkerIdentity>
      INDEX_IDENTITY = IndexDefinition.ofUnique(Pair::getFirst);
  protected static final IndexDefinition<Pair<WorkerIdentity, WorkerInfo>, String> INDEX_STATE =
      IndexDefinition.ofNonUnique(workerHolder -> workerHolder.getSecond().getState());

  protected final IndexedSet<Pair<WorkerIdentity, WorkerInfo>> mWorkers =
      new IndexedSet<>(INDEX_IDENTITY, INDEX_STATE);

  /**
   * Lazily computed. This object is immutable, so synchronization is unnecessary.
   */
  private int mHashCode = 0;

  public WorkerClusterView(Iterable<WorkerInfo> workers) {
    for (WorkerInfo worker : workers) {
      final Pair<WorkerIdentity, WorkerInfo> pair =
          new Pair<>(WorkerIdentity.definedByNetAddress(worker.getAddress()), worker);
      mWorkers.add(pair);
    }
  }

  public static WorkerClusterView empty() {
    return new WorkerClusterView(Collections.emptyList());
  }

  /**
   * Checks if this view and the other view have exactly the same number of workers,
   * and the workers have the same identities.
   *
   * @param other the other view to compare with
   * @return whether the two views are equal
   */
  public final boolean isEqualTo(WorkerClusterView other) {
    if (getNumWorkers() != other.getNumWorkers()) {
      return false;
    }
    if (hashCode() != other.hashCode()) {
      return false;
    }
    return this.workerIdentitySetContains(other) && other.workerIdentitySetContains(this);
  }

  protected boolean workerIdentitySetContains(WorkerClusterView other) {
    Iterator<WorkerIdentity> otherWorkerIdentities =
        other.mWorkers.stream().map(Pair::getFirst).iterator();
    while (otherWorkerIdentities.hasNext()) {
      if (!mWorkers.contains(INDEX_IDENTITY, otherWorkerIdentities.next())) {
        return false;
      }
    }
    return true;
  }

  public final boolean isEmpty() {
    return mWorkers.isEmpty();
  }

  public final int getNumWorkers() {
    return mWorkers.size();
  }

  public WorkerClusterView getLiveWorkers() {
    // TODO(bowen): replace "LIVE" with an enum
    return new WorkerClusterView(
        mWorkers.getByField(INDEX_STATE, "LIVE").stream().map(Pair::getSecond)::iterator);
  }

  public WorkerClusterView getLostWorkers() {
    // TODO(bowen): replace "LOST" with an enum
    return new WorkerClusterView(
        mWorkers.getByField(INDEX_STATE, "LOST").stream().map(Pair::getSecond)::iterator);
  }

  public final Iterator<Pair<WorkerIdentity, WorkerInfo>> iterateWorkers() {
    return Iterators.unmodifiableIterator(mWorkers.iterator());
  }

  public final Optional<WorkerInfo> getWorkerInfo(WorkerIdentity workerIdentity) {
    return Optional.ofNullable(mWorkers.getFirstByField(INDEX_IDENTITY, workerIdentity))
        .map(Pair::getSecond);
  }

  @Override
  public int hashCode() {
    int h = mHashCode;
    if (h == 0 && getNumWorkers() > 0) {
      h = 1;
      Iterator<Pair<WorkerIdentity, WorkerInfo>> iterator = iterateWorkers();
      while (iterator.hasNext()) {
        WorkerIdentity workerIdentity = iterator.next().getFirst();
        h = h * 31 + (workerIdentity == null ? 0 : workerIdentity.hashCode());
      }
      mHashCode = h;
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof WorkerClusterView)) {
      return false;
    }
    WorkerClusterView other = (WorkerClusterView) obj;
    return this.workerIdentitySetContains(other) && other.workerIdentitySetContains(this);
  }
}
