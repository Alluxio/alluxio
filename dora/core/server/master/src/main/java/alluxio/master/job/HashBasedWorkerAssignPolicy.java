package alluxio.master.job;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.wire.WorkerInfo;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class HashBasedWorkerAssignPolicy extends WorkerAssignPolicy {
  WorkerLocationPolicy mWorkerLocationPolicy = new WorkerLocationPolicy(2000);

  @Override
  protected WorkerInfo pickAWorker(String object, @Nullable Collection<WorkerInfo> workerInfos) {
    if (workerInfos == null) {
      return null;
    }
    List<BlockWorkerInfo> candidates = workerInfos.stream()
        .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
        .collect(toList());
    List<BlockWorkerInfo> blockWorkerInfo = mWorkerLocationPolicy
        .getPreferredWorkers(candidates, object, 1);
    if (blockWorkerInfo.isEmpty()) {
      return null;
    }
    WorkerInfo returnWorker = workerInfos.stream().filter(workerInfo ->
            workerInfo.getAddress().equals(blockWorkerInfo.get(0).getNetAddress()))
        .findFirst().get();
    return returnWorker;
  }
}
