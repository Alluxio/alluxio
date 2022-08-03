package alluxio.master.file.meta.cross.cluster;

import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.MountList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a thread that will keep the cross cluster configuration service
 * up to date when local mount lists change.
 */
public class CrossClusterMountClientRunner implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMountClientRunner.class);

  private final CrossClusterClient mClient;
  private final AtomicReference<MountList> mMountList = new AtomicReference<>();
  private final Thread mRunner;
  private volatile boolean mDone = false;

  /**
   * @param client the client to the cross cluster configuration service
   */
  public CrossClusterMountClientRunner(CrossClusterClient client) {
    mClient = client;
    mRunner = new Thread(() -> {
      while (!mDone) {
        MountList next = mMountList.get();
        if (next != null) {
          try {
            mClient.setMountList(next);
            mMountList.compareAndSet(next, null);
          } catch (AlluxioStatusException e) {
            LOG.warn("Error while trying to update cross cluster mount list", e);
          }
        }
        try {
          synchronized (this) {
            if (mMountList.get() == null) {
              wait();
            }
          }
        } catch (InterruptedException e) {
          // loop again, to see if we are done
        }
      }
    });
  }

  /**
   * Starts running the service that will keep the configuration service
   * up to date with the local cluster mount changes.
   */
  public void start() {
    mRunner.start();
  }

  /**
   * Called when a local mount changes.
   * @param mountList the new local mount state
   */
  public void onLocalMountChange(MountList mountList) {
    mMountList.set(mountList);
    synchronized (this) {
      notifyAll();
    }
  }

  @Override
  public void close() throws IOException {
    mDone = true;
    mRunner.interrupt();
    try {
      mRunner.join(5000);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for runner to complete", e);
    }
  }
}
