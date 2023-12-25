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

import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.curator.shaded.com.google.common.collect.Streams;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.concurrent.Immutable;

/**
 * Snapshot for a cluster view.
 */
@Immutable
public final class WorkerClusterView implements Iterable<WorkerInfo> {
  private final Instant mInstantCreated;
  private final Map<WorkerIdentity, WorkerInfo> mWorkers;

  /**
   * Creates a cluster view with the give workers.
   *
   * @param workers workers in this view
   */
  public WorkerClusterView(Iterable<WorkerInfo> workers) {
    this(workers, Instant.now());
  }

  @VisibleForTesting
  WorkerClusterView(Iterable<WorkerInfo> workers, Instant createdTime) {
    mWorkers = Streams.stream(workers)
        .collect(ImmutableMap.toImmutableMap(
            WorkerInfo::getIdentity,
            Function.identity(),
            (first, second) -> {
              throw new IllegalArgumentException(
                  String.format("duplicate workers with the same ID: first: %s, second: %s",
                      first, second));
            }));
    // Note Instant.now() uses the system clock and is NOT monotonic
    // which is fine because we want to invalidate stale snapshots based on wall clock time
    mInstantCreated = createdTime;
  }

  /**
   * Finds a worker by its ID.
   *
   * @param workerIdentity the ID of the worker to find
   * @return the worker info of the given worker, or none if the worker is not found
   */
  public Optional<WorkerInfo> getWorkerById(WorkerIdentity workerIdentity) {
    return Optional.ofNullable(mWorkers.get(workerIdentity));
  }

  @Override
  public Iterator<WorkerInfo> iterator() {
    return Iterators.unmodifiableIterator(mWorkers.values().iterator());
  }

  /**
   * Converts to a stream of {@link WorkerInfo}.
   *
   * @return stream of workers
   */
  public Stream<WorkerInfo> stream() {
    return mWorkers.values().stream();
  }

  /**
   * @return set of IDs of workers contained in this view
   */
  public Set<WorkerIdentity> workerIds() {
    return mWorkers.keySet();
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
   * @return the time when this snapshot was created
   */
  public Instant getSnapshotTime() {
    return mInstantCreated;
  }

  /**
   * Note that the equals implementation considers the creation timestamp to be part of the
   * snapshot's identity. To compare two snapshots only by the contained workers,
   * use {@link #workerIds()} to compare only the IDs, or
   * use {@link com.google.common.collect.Iterables#elementsEqual(Iterable, Iterable)} to compare
   * all fields in {@link WorkerInfo}.
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("InstantCreated", mInstantCreated)
        .add("Workers", mWorkers)
        .toString();
  }
}
