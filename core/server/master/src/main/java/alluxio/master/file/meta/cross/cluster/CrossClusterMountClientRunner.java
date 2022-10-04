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

package alluxio.master.file.meta.cross.cluster;

import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.collections.Pair;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ErrorType;
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.resource.LockResource;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs a thread that will keep the cross cluster configuration service
 * {@link alluxio.master.cross.cluster.CrossClusterState} up to date when local mount lists change.
 * This runs a thread that continues to update the configuration service in case of
 * failures.
 */
public class CrossClusterMountClientRunner implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountClientRunner.class);

  private CrossClusterClient mRunnerClient;
  private CrossClusterClient mMountChangeClient;
  private final AtomicReference<Pair<Boolean, MountList>> mMountList =
      new AtomicReference<>(new Pair<>(true, null));
  private Thread mRunner;
  private volatile boolean mDone = false;
  private volatile boolean mStopped = true;
  private volatile boolean mThreadChange = false;
  private final Lock mClientChangeLock = new ReentrantLock();

  /**
   * Create a new cross cluster runner that will keep the configuration service up to date
   * with local mount changes. The runner client will continually try to update the configuration
   * service in case of failover or reconnection, while the mount change client will be used
   * when initially adding a new mount.
   * @param runnerClient the client to maintain an up-to-date state with the configuration service
   * @param mountChangeClient the client used to update the service when adding new mounts
   */
  public CrossClusterMountClientRunner(
      CrossClusterClient runnerClient, CrossClusterClient mountChangeClient) {
    mRunnerClient = runnerClient;
    mMountChangeClient = mountChangeClient;
    mRunner = new Thread(this::doRun, "CrossClusterMountRunner");
  }

  /**
   * Initializes the runner thread, should be called before
   * {@link CrossClusterMountClientRunner#start()}.
   */
  public void run() {
    mRunner.start();
  }

  private void doRun() {
    while (true) {
      try {
        synchronized (this) {
          while (mMountList.get().getFirst() || mStopped) {
            if (mDone || mThreadChange) {
              return;
            }
            wait();
          }
        }
      } catch (InterruptedException e) {
        continue;
      }
      if (mDone || mThreadChange) {
        return;
      }
      Pair<Boolean, MountList> next = mMountList.get();
      if (!next.getFirst()) {
        try {
          mRunnerClient.setMountList(next.getSecond());
          mMountList.compareAndSet(next, new Pair<>(true, next.getSecond()));
        } catch (Exception e) {
          LOG.warn("Error while trying to update cross cluster mount list", e);
          // disconnect the client as this could be caused by
          // a channel authentication error
          // if the exception is an UnavailableException then we don't close the
          // connection as the Subscriber may be trying to concurrently connect
          if (!(e instanceof UnavailableException)) {
            mRunnerClient.disconnect();
          }
        }
      }
    }
  }

  /**
   * Change the client for the runner. This will interrupt the running thread,
   * start a new one, and send the most recently updated mount list to the new
   * recipient.
   * @param runnerClient the new runner client
   * @param mountChangeClient the new mount change client
   */
  public void changeClient(CrossClusterClient runnerClient, CrossClusterClient mountChangeClient) {
    synchronized (this) {
      mMountChangeClient = mountChangeClient;
    }
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      if (mDone) {
        return;
      }
      mThreadChange = true;
      mRunner.interrupt();
      try {
        mRunner.join();
      } catch (InterruptedException e) {
        LOG.warn("Error while waiting for runner thread to stop", e);
      }
      mThreadChange = false;
      mRunnerClient = runnerClient;
      Pair<Boolean, MountList> mountList = mMountList.get();
      if (mountList.getSecond() != null) {
        mMountList.compareAndSet(mountList, new Pair<>(false, mountList.getSecond()));
      }
      mRunner = new Thread(this::doRun, "CrossClusterMountRunner");
      mRunner.start();
    }
  }

  /**
   * Starts running the service that will keep the configuration service
   * up to date with the local cluster mount changes.
   */
  public void start() {
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      synchronized (this) {
        mStopped = false;
        notifyAll();
      }
    }
  }

  /**
   * Stops the service that keeps the configuration service
   * up to date with the local cluster mount changes.
   */
  public void stop() {
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      synchronized (this) {
        mStopped = true;
        notifyAll();
      }
    }
  }

  /**
   * This will synchronously update the mount on the naming service, or throw
   * an {@link AlluxioRuntimeException} if unable to do this.
   * This should be called before a cross cluster mount is added, as we must
   * be able to update the naming service before mounting.
   * @param mountList the new mount list
   */
  public void beforeLocalMountChange(MountList mountList) {
    try {
      mMountChangeClient.setMountList(mountList);
    } catch (AlluxioStatusException e) {
      throw new AlluxioRuntimeException(Status.DEADLINE_EXCEEDED,
          String.format("Failed to update mount list %s on cross cluster naming service",
              mountList), e, ErrorType.Internal, false);
    }
  }

  /**
   * This should be called if a connection to the configuration service is dropped and
   * remade as this may indicate a failure of the configuration service, so it
   * will need to be resent the current mount list.
   */
  public synchronized void onReconnection() {
    Pair<Boolean, MountList> mountList = mMountList.get();
    if (mountList != null && mountList.getSecond() != null) {
      mMountList.set(new Pair<>(false, mountList.getSecond()));
      notifyAll();
    }
  }

  /**
   * Called when a local mount changes.
   * @param mountList the new local mount state
   */
  public synchronized void onLocalMountChange(MountList mountList) {
    mMountList.set(new Pair<>(false, mountList));
    notifyAll();
  }

  @Override
  public void close() throws IOException {
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      synchronized (this) {
        mStopped = true;
        mDone = true;
        notifyAll();
      }
      try {
        mRunner.join(5000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for runner to complete", e);
      }
    }
  }
}
