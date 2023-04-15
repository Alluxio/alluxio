package alluxio.master.job;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerInfo;

import java.util.Collection;
import java.util.List;

public abstract class WorkerAssignPolicy {

  protected abstract WorkerInfo pickAWorker(String object, Collection<WorkerInfo> workerInfos);
}
