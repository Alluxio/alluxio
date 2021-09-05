package alluxio.master.block;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.resource.LockResource;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;

public class WorkerRegisterContext implements Closeable {
  long mWorkerId;
  LockResource mWorkerLock;
  MasterWorkerInfo mWorker;
  BlockMaster mBlockMaster;

  public static WorkerRegisterContext create(BlockMaster blockMaster, long workerId) throws NotFoundException {
    WorkerRegisterContext context = new WorkerRegisterContext();
    context.mBlockMaster = blockMaster;
    context.mWorkerId = workerId;

    MasterWorkerInfo info = blockMaster.getWorker(workerId);
    context.mWorker = info;

    context.mWorkerLock = info.lockWorkerMeta(EnumSet.of(
            WorkerMetaLockSection.STATUS,
            WorkerMetaLockSection.USAGE,
            WorkerMetaLockSection.BLOCKS), false);
    return context;
  }

  @Override
  public void close() throws IOException {
    if (mBlockMaster != null && mWorkerLock != null) {
      mBlockMaster.unlockWorker(mWorkerLock);
    }
  }
}
