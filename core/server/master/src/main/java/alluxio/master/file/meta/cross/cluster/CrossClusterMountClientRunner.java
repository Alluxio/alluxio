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
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.resource.LockResource;

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
 */
public class CrossClusterMountClientRunner implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountClientRunner.class);

  private CrossClusterClient mClient;
  private final AtomicReference<Pair<Boolean, MountList>> mMountList =
      new AtomicReference<>(new Pair<>(true, null));
  private Thread mRunner;
  private volatile boolean mDone = false;
  private volatile boolean mStopped = true;
  private volatile boolean mThreadChange = false;
  private final Lock mClientChangeLock = new ReentrantLock();

  /**
   * @param client the client to the cross cluster configuration service
   */
  public CrossClusterMountClientRunner(CrossClusterClient client) {
    mClient = client;
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
          // TODO(tcrain) should resend mount list from time to time in case it is not reliable?
          mClient.setMountList(next.getSecond());
          mMountList.compareAndSet(next, new Pair<>(true, next.getSecond()));
        } catch (Exception e) {
          LOG.warn("Error while trying to update cross cluster mount list", e);
          mClient.disconnect();
        }
      }
    }
  }

  /**
   * Change the client for the runner. This will interrupt the running thread,
   * start a new one, and send the most recently updated mount list to the new
   * recipient.
   * @param client the new client
   */
  public void changeClient(CrossClusterClient client) {
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
      mClient = client;
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
