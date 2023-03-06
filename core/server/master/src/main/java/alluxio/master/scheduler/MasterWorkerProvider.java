package alluxio.master.scheduler;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMaster;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.WorkerProvider;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

public class MasterWorkerProvider implements WorkerProvider {
  private final FileSystemMaster mFileSystemMaster;
  private final FileSystemContext mContext;

  public MasterWorkerProvider(FileSystemMaster fileSystemMaster, FileSystemContext context) {
    mFileSystemMaster = fileSystemMaster;
    mContext = context;
  }


  @Override
  public List<WorkerInfo> getWorkerInfos() {
    try {
      return  mFileSystemMaster.getWorkerInfoList();
    } catch (UnavailableException e) {
      throw new UnavailableRuntimeException(
          "fail to get worker infos because master is not available", e);
    }
  }

  @Override
  public CloseableResource<BlockWorkerClient> getWorkerClient(WorkerNetAddress address) {
    try {
      return mContext.acquireBlockWorkerClient(address);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }
}
