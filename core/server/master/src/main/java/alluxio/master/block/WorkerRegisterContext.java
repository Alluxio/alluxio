package alluxio.master.block;

import alluxio.exception.status.NotFoundException;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.resource.LockResource;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.time.Instant;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This holds the context while the worker is registering with the master with a stream.
 * This context is initialized when the stream starts and closed when the stream is
 * either completed or aborted.
 */
public class WorkerRegisterContext implements Closeable {
  /** Reference to the worker's metadata in the {@link BlockMaster} */
  MasterWorkerInfo mWorker;
  /**
   * Locks on the worker's metadata sections. The locks will be held throughout the
   * stream and will be unlocked at the end.
   */
  private LockResource mWorkerLock;
  private AtomicBoolean mOpen;
  StreamObserver<RegisterWorkerPRequest> mRequestObserver;
  StreamObserver<RegisterWorkerPResponse> mResponseObserver;

  long mLastActivityTimeMs;

  private WorkerRegisterContext(MasterWorkerInfo info,
                        StreamObserver<RegisterWorkerPRequest> requestObserver,
                        StreamObserver<RegisterWorkerPResponse> responseObserver) {
    mWorker = info;
    mRequestObserver = requestObserver;
    mResponseObserver = responseObserver;
    System.out.println("Acquiring all worker locks for " + getWorkerId());
    mWorkerLock = info.lockWorkerMeta(EnumSet.of(
            WorkerMetaLockSection.STATUS,
            WorkerMetaLockSection.USAGE,
            WorkerMetaLockSection.BLOCKS), false);
    System.out.println("Acquired all worker locks for " + getWorkerId());

    mOpen = new AtomicBoolean(true);
  }

  public long getWorkerId() {
    return mWorker.getId();
  }

  public boolean isOpen() {
    return mOpen.get();
  }

  public void updateTs() {
    mLastActivityTimeMs = Instant.now().toEpochMilli();
  }

  public long getLastActivityTimeMs() {
    return mLastActivityTimeMs;
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
    System.out.println("Context closed");
  }

  public static synchronized WorkerRegisterContext create(
          BlockMaster blockMaster, long workerId,
          StreamObserver<RegisterWorkerPRequest> requestObserver,
          StreamObserver<RegisterWorkerPResponse> responseObserver) throws NotFoundException {
    MasterWorkerInfo info = blockMaster.getWorker(workerId);
    return new WorkerRegisterContext(info, requestObserver, responseObserver);
  }
}
