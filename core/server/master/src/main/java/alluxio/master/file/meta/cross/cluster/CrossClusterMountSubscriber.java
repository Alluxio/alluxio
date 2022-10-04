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
import alluxio.grpc.ClusterId;
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.resource.LockResource;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Subscribes to the cross cluster configuration service
 * {@link alluxio.master.cross.cluster.CrossClusterState}, receiving notifications when
 * a cross cluster enabled mount changes at an external cluster.
 * This class runs a thread that will maintain a connection to the configuration service.
 */
public class CrossClusterMountSubscriber implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountSubscriber.class);

  private CrossClusterClient mCrossClusterClient;
  private Thread mRunner;

  private final CrossClusterMount mCrossClusterMount;
  private final String mClusterId;
  private volatile boolean mDone = false;
  private volatile boolean mStopped = true;
  private volatile boolean mThreadChange = false;
  private MountChangeStream mMountChangeStream;
  private final Lock mClientChangeLock = new ReentrantLock();
  private final Runnable mOnConnection;

  /**
   * @param clusterId          the local cluster id
   * @param crossClusterClient the client used to receive updates
   * @param crossClusterMount  the object creating subscriptions to other clusters
   * @param onConnection a function to call when a connection to the service is made
   */
  public CrossClusterMountSubscriber(
      String clusterId, CrossClusterClient crossClusterClient,
      CrossClusterMount crossClusterMount, Runnable onConnection) {
    mClusterId = clusterId;
    mCrossClusterClient = crossClusterClient;
    mCrossClusterMount = crossClusterMount;
    mOnConnection = onConnection;
    mRunner = new Thread(this::doRun, "CrossClusterMountSubscriber");
  }

  /**
   * Initializes the runner thread, should be called before
   * {@link CrossClusterMountSubscriber#start()}.
   */
  public void run() {
    mRunner.start();
  }

  /**
   * Change the client connecting to the configuration service.
   * It will interrupt the running thread and start a new one.
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
      synchronized (this) {
        mCrossClusterClient = client;
        if (mMountChangeStream != null) {
          mMountChangeStream.cancel();
        }
      }
      mRunner = new Thread(this::doRun, "CrossClusterMountSubscriber");
      mRunner.start();
    }
  }

  private void doRun() {
    while (true) {
      try {
        synchronized (this) {
          while (mMountChangeStream != null || mStopped) {
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
      try {
        CrossClusterClient client;
        MountChangeStream stream;
        synchronized (this) {
          mMountChangeStream = new MountChangeStream();
          stream = mMountChangeStream;
          client = mCrossClusterClient;
        }
        client.subscribeMounts(mClusterId, stream);
        mOnConnection.run();
      } catch (Exception e) {
        LOG.warn("Error connecting to cross cluster configuration service", e);
        synchronized (this) {
          mMountChangeStream = null;
        }
      }
    }
  }

  /**
   * Start the thread to maintain the subscription to the configuration service.
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
   * Stops the thread to maintain the subscription to the configuration service.
   */
  public void stop() {
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      synchronized (this) {
        mStopped = true;
        notifyAll();
      }
    }
  }

  private class MountChangeStream implements ClientResponseObserver<ClusterId, MountList> {

    ClientCallStreamObserver<ClusterId> mRequestStream;

    @Override
    public void onNext(MountList value) {
      try {
        mCrossClusterMount.setExternalMountList(value);
      } catch (UnknownHostException e) {
        LOG.error("Error updating mount list", e);
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Stream error to cross cluster configuration service", t);
      synchronized (CrossClusterMountSubscriber.this) {
        if (mMountChangeStream == this) {
          mMountChangeStream = null;
          CrossClusterMountSubscriber.this.notifyAll();
          // disconnect and reconnect the client as this could be caused by
          // a channel authentication error
          mCrossClusterClient.disconnect();
        }
      }
    }

    @Override
    public void onCompleted() {
      LOG.error("Stream to cross cluster configuration service completed");
      synchronized (CrossClusterMountSubscriber.this) {
        if (mMountChangeStream == this) {
          mMountChangeStream = null;
          CrossClusterMountSubscriber.this.notifyAll();
        }
      }
    }

    @Override
    public synchronized void beforeStart(ClientCallStreamObserver<ClusterId> requestStream) {
      mRequestStream = requestStream;
    }

    public synchronized void cancel() {
      if (mRequestStream != null) {
        mRequestStream.cancel("Cross cluster subscription cancelled", null);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try (LockResource ignored = new LockResource(mClientChangeLock)) {
      MountChangeStream stream;
      synchronized (this) {
        stream = mMountChangeStream;
        mMountChangeStream = null;
        mDone = true;
        // we must interrupt the thread as it will stop the client in the connection process
        mRunner.interrupt();
        notifyAll();
      }
      // cancel the stream outsize the synchronized block
      if (stream != null) {
        stream.cancel();
      }
      try {
        mRunner.join(5000);
      } catch (InterruptedException e) {
        LOG.warn("Error waiting for runner thread to stop", e);
      }
    }
  }
}
