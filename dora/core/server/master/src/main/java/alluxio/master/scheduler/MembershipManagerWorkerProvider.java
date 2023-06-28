package alluxio.master.scheduler;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.membership.MembershipManager;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.WorkerProvider;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MembershipManagerWorkerProvider implements WorkerProvider {
  private final MembershipManager mMembershipManager;
  private final FileSystemContext mContext;

  public MembershipManagerWorkerProvider(MembershipManager membershipMgr, FileSystemContext context) {
    mMembershipManager = membershipMgr;
    mContext = context;
  }

  @Override
  public List<WorkerInfo> getWorkerInfos() {
    return mMembershipManager.getAllMembers();
  }

  @Override
  public List<WorkerInfo> getLiveWorkerInfos() {
    return mMembershipManager.getLiveMembers();
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
