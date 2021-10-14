package alluxio.master.block;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.RegisterWorkerStreamPRequest;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.resource.LockResource;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerRegisterContext implements Closeable {
  long mWorkerId;
  LockResource mWorkerLock;
  MasterWorkerInfo mWorker;
  BlockMaster mBlockMaster;
  AtomicBoolean mOpen;
  StreamObserver<RegisterWorkerStreamPRequest> mRequestObserver;
  StreamObserver<RegisterWorkerStreamPResponse> mResponseObserver;

  long mLastUpdatedTime;

  private WorkerRegisterContext(MasterWorkerInfo info,
                        StreamObserver<RegisterWorkerStreamPRequest> requestObserver,
                        StreamObserver<RegisterWorkerStreamPResponse> responseObserver) {
    mWorker = info;
    mWorkerId = info.getId();
    mRequestObserver = requestObserver;
    mResponseObserver = responseObserver;
    System.out.println("Acquiring all worker locks for " + mWorkerId);
    mWorkerLock = info.lockWorkerMeta(EnumSet.of(
            WorkerMetaLockSection.STATUS,
            WorkerMetaLockSection.USAGE,
            WorkerMetaLockSection.BLOCKS), false);
    System.out.println("Acquired all worker locks for " + mWorkerId);

    mOpen = new AtomicBoolean(true);
  }

  public static synchronized WorkerRegisterContext create(BlockMaster blockMaster, long workerId,
                                                          StreamObserver<RegisterWorkerStreamPRequest> requestObserver,
                                                          StreamObserver<RegisterWorkerStreamPResponse> responseObserver) throws NotFoundException {
    MasterWorkerInfo info = blockMaster.getWorker(workerId);

    WorkerRegisterContext context = new WorkerRegisterContext(info, requestObserver, responseObserver);
    return context;
  }

  public boolean isOpen() {
    return mOpen.get();
  }

  public void updateTs() {
    mLastUpdatedTime = Instant.now().toEpochMilli();
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(mOpen.get(), "The context is already closed!");

    if (mWorkerLock != null) {
      mWorkerLock.close();
    }
    mOpen.set(false);
  }
}