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

package alluxio.membership;

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Iterators;

import java.time.Instant;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.concurrent.Immutable;

/**
 * Snapshot for a cluster view.
 */
@Immutable
public final class WorkerClusterView implements Iterable<WorkerInfo> {
  private final Instant mInstantCreated;
  private final IndexedSet<WorkerInfo> mWorkers;

  private static final IndexDefinition<WorkerInfo, WorkerIdentity> INDEX_WORKER_ID =
      IndexDefinition.ofUnique(WorkerInfo::getIdentity);

  /**
   * Creates a cluster view with the give workers.
   *
   * @param workers workers in this view
   */
  public WorkerClusterView(Iterable<WorkerInfo> workers) {
    mWorkers = new IndexedSet<>(INDEX_WORKER_ID);
    for (WorkerInfo workerInfo : workers) {
      mWorkers.add(workerInfo);
    }
    // Note Instant.now() uses the system clock and is NOT monotonic
    // which is fine because we want to invalidate stale snapshots based on wall clock time
    mInstantCreated = Instant.now();
  }

  public Optional<WorkerInfo> getWorkerById(WorkerIdentity workerIdentity) {
    return Optional.ofNullable(
        mWorkers.getFirstByField(INDEX_WORKER_ID, workerIdentity));
  }

  @Override
  public Iterator<WorkerInfo> iterator() {
    return Iterators.unmodifiableIterator(mWorkers.iterator());
  }

  /**
   * Converts to a stream of {@link WorkerInfo}.
   *
   * @return stream of workers
   */
  public Stream<WorkerInfo> stream() {
    return mWorkers.stream();
  }

  /**
   * @return number of workers contained in the cluster view
   */
  public int size() {
    return mWorkers.size();
  }

  /**
   * @return if the cluster contains no worker
   */
  public boolean isEmpty() {
    return mWorkers.isEmpty();
  }

  /**
   * @return the time when this snapshot was created.
   */
  public Instant getSnapshotTime() {
    return mInstantCreated;
  }

  /**
   * Note that the equals implementation considers the creation timestamp to be part of the
   * snapshot's identity. To compare two snapshots only by the contained workers,
   * use {@link com.google.common.collect.Iterables#elementsEqual(Iterable, Iterable)}.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerClusterView that = (WorkerClusterView) o;
    return Objects.equals(mInstantCreated, that.mInstantCreated)
        && Objects.equals(mWorkers, that.mWorkers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mInstantCreated, mWorkers);
  }
}
