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

package alluxio.master.block;

import static java.util.Objects.requireNonNull;

import alluxio.exception.status.NotFoundException;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.resource.LockResource;

import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.time.Clock;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This holds the context while the worker is registering with the master with a stream.
 * This context is initialized when the stream starts and closed when the stream is
 * either completed or aborted.
 */
public class WorkerRegisterContext implements Closeable {
  /** Reference to the worker's metadata in the {@link BlockMaster}. */
  private final MasterWorkerInfo mWorkerInfo;

  /**
   * Locks on the worker's metadata sections. The locks will be held throughout the
   * stream and will be unlocked at the end.
   */
  private final LockResource mWorkerLock;
  private final AtomicBoolean mOpen = new AtomicBoolean(true);
  private final StreamObserver<RegisterWorkerPRequest> mWorkerRequestObserver;
  private final Clock mClock;

  /**
   * Keeps track of the last activity time on this stream.
   * Streams that have been inactive for some time will be recycled by
   * {@link alluxio.master.block.DefaultBlockMaster.WorkerRegisterStreamGCExecutor}.
   */
  private long mLastActivityTimeMs;

  private WorkerRegisterContext(
      MasterWorkerInfo workerInfo,
      StreamObserver<RegisterWorkerPRequest> workerRequestObserver,
      Clock clock) {
    mWorkerInfo = requireNonNull(workerInfo, "workerInfo is null");
    mWorkerRequestObserver = requireNonNull(workerRequestObserver,
        "mWorkerRequestObserver is null");
    mWorkerLock = workerInfo.lockWorkerMeta(EnumSet.of(
        WorkerMetaLockSection.STATUS,
        WorkerMetaLockSection.USAGE,
        WorkerMetaLockSection.BLOCKS), false);
    mClock = requireNonNull(clock, "clock is null");
    mLastActivityTimeMs = mClock.millis();
  }

  /**
   * Checks whether the stream is still open.
   * If not open, no more operation should be permitted on the stream,
   * and this stream has been closed and all the locks have been released.
   *
   * @return whether the stream is still open
   */
  public boolean isOpen() {
    return mOpen.get();
  }

  void updateTs() {
    mLastActivityTimeMs = mClock.millis();
  }

  long getLastActivityTimeMs() {
    return mLastActivityTimeMs;
  }

  void closeWithError(Exception e) {
    mWorkerRequestObserver.onError(e);
  }

  @Override
  public void close() {
    if (!mOpen.get()) {
      return;
    }
    if (mWorkerLock != null) {
      mWorkerLock.close();
    }
    mOpen.set(false);
  }

  /**
   * Creates a new {@link WorkerRegisterContext}.
   *
   * @param blockMaster the block master
   * @param workerId the worker ID
   * @param workerRequestObserver receives requests from the worker
   * @return a new {@link WorkerRegisterContext}
   */
  public static synchronized WorkerRegisterContext create(
      BlockMaster blockMaster, long workerId,
      StreamObserver<RegisterWorkerPRequest> workerRequestObserver) throws NotFoundException {
    MasterWorkerInfo workerInfo = blockMaster.getWorker(workerId);
    return new WorkerRegisterContext(workerInfo, workerRequestObserver, blockMaster.getClock());
  }

  /**
   * @return MasterWorkerInfo worker's runtime info and metadata
   */
  public MasterWorkerInfo getWorkerInfo() {
    return mWorkerInfo;
  }
}
