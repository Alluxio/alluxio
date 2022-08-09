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
import alluxio.grpc.MountList;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Subscribes to the cross cluster configuration service
 * {@link alluxio.master.cross.cluster.CrossClusterState}, receiving notifications when
 * a cross cluster enabled mount changes at an external cluster.
 */
public class CrossClusterMountSubscriber implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountSubscriber.class);

  private final CrossClusterClient mCrossClusterClient;
  private final CrossClusterMount mCrossClusterMount;
  private final Thread mRunner;
  private volatile boolean mDone = false;
  private volatile boolean mStopped = true;
  private MountChangeStream mMountChangeStream;

  /**
   * @param clusterId          the local cluster id
   * @param crossClusterClient the client used to receive updates
   * @param crossClusterMount  the object creating subscriptions to other clusters
   */
  public CrossClusterMountSubscriber(String clusterId, CrossClusterClient crossClusterClient,
                                     CrossClusterMount crossClusterMount) {
    mCrossClusterClient = crossClusterClient;
    mCrossClusterMount = crossClusterMount;
    mRunner = new Thread(() -> {
      while (true) {
        try {
          synchronized (this) {
            while (mMountChangeStream != null || mStopped) {
              if (mDone) {
                return;
              }
              wait();
            }
          }
        } catch (InterruptedException e) {
          continue;
        }
        if (mDone) {
          return;
        }
        try {
          synchronized (this) {
            mMountChangeStream = new MountChangeStream();
          }
          mCrossClusterClient.subscribeMounts(clusterId, mMountChangeStream);
        } catch (Exception e) {
          LOG.warn("Error connecting to cross cluster configuration service", e);
          synchronized (this) {
            mMountChangeStream = null;
          }
        }
      }
    }, "CrossClusterMountSubscriber");
    mRunner.start();
  }

  /**
   * Start the thread to maintain the subscription to the configuration service.
   */
  public void start() {
    synchronized (this) {
      mStopped = false;
      notifyAll();
    }
  }

  /**
   * Stops the thread to maintain the subscription to the configuration service.
   */
  public void stop() {
    synchronized (this) {
      mStopped = true;
      notifyAll();
    }
  }

  private class MountChangeStream implements ClientResponseObserver<ClusterId, MountList> {

    ClientCallStreamObserver<ClusterId> mRequestStream;

    @Override
    public void onNext(MountList value) {
      mCrossClusterMount.setExternalMountList(value);
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Stream error to cross cluster configuration service", t);
      synchronized (CrossClusterMountSubscriber.this) {
        if (mMountChangeStream == this) {
          mMountChangeStream = null;
          CrossClusterMountSubscriber.this.notifyAll();
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
