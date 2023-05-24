package alluxio.master.job;

import alluxio.wire.WorkerInfo;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Policy which employs Round Robin around given workers to select worker.
 */
public class RoundRobinWorkerAssignPolicy extends WorkerAssignPolicy {
  private AtomicInteger mCounter = new AtomicInteger(0);

  @Override
  protected WorkerInfo pickAWorker(String object, Collection<WorkerInfo> workerInfos) {
    if (workerInfos.isEmpty()) {
      return null;
    }
    int nextWorkerIdx = Math.floorMod(mCounter.incrementAndGet(), workerInfos.size());
    WorkerInfo pickedWorker = workerInfos.toArray(new WorkerInfo[workerInfos.size()])
        [nextWorkerIdx];
    return pickedWorker;
  }
}
