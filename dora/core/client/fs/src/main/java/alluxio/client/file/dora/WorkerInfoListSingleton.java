package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;

import java.util.ArrayList;
import java.util.List;

public class WorkerInfoListSingleton {
  private static WorkerInfoListSingleton instance;
  private List<BlockWorkerInfo> mWorkerInfoList;
  private WorkerInfoListSingleton() {
    mWorkerInfoList = new ArrayList<>();
  }
  public static synchronized WorkerInfoListSingleton getInstance() {
    if (instance == null) {
      instance = new WorkerInfoListSingleton();
    }
    return instance;
  }
  public boolean isEmpty() {
    return mWorkerInfoList.isEmpty();
  }

  public synchronized void initWorkerList(List<BlockWorkerInfo> workerList) {
    mWorkerInfoList = workerList;
  }
  public List<BlockWorkerInfo> getWorkerList() {
    return mWorkerInfoList;
  }

  public synchronized void roulette() {
    if (!mWorkerInfoList.isEmpty()) {
      BlockWorkerInfo firstWorker = mWorkerInfoList.remove(0);
      mWorkerInfoList.add(firstWorker);
    }
  }
}
