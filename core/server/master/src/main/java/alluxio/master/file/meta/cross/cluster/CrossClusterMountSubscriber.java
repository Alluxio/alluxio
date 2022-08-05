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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.MountList;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Subscribes to the cross cluster configuration service {@link alluxio.master.cross.cluster.CrossClusterState}, receiving notifications when
 * a cross cluster enabled mount changes at an external cluster.
 */
public class CrossClusterMountSubscriber implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountSubscriber.class);

  private final CrossClusterClient mCrossClusterClient;
  private final CrossClusterMount mCrossClusterMount;
  private final Thread mRunner;
  private volatile boolean mDone = false;
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
        synchronized (this) {
          while (mMountChangeStream != null) {
            try {
              if (mDone) {
                return;
              }
              wait();
            } catch (InterruptedException e) {
              // retry the loop again
            }
          }
          if (mDone) {
            return;
          }
          try {
            mMountChangeStream = new MountChangeStream();
            mCrossClusterClient.subscribeMounts(clusterId, mMountChangeStream);
          } catch (AlluxioStatusException e) {
            LOG.warn("Error connecting to cross cluster configuration service", e);
            synchronized (this) {
              mMountChangeStream = null;
            }
          }
        }
      }
    }, "CrossClusterMountSubscriber");
  }

  /**
   * Start the thread to maintain the subscription to the configuration service.
   */
  public void start() {
    mRunner.start();
  }

  private class MountChangeStream implements StreamObserver<MountList> {
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
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      mDone = true;
    }
    try {
      mRunner.join(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
