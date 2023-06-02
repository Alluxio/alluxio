package alluxio.worker.block;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.membership.EtcdClient;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.MoreObjects;
import io.etcd.jetcd.ByteSequence;

import java.util.concurrent.atomic.AtomicReference;

public class BlockEtcdSync implements HeartbeatExecutor {
//  EtcdClient mEtcdClient;


  public BlockEtcdSync() {
//    mEtcdClient = new EtcdClient();
//    mEtcdClient.connect();
  }

  public static class WorkerService extends EtcdClient.ServiceEntityContext {
    AtomicReference<Long> mWorkerId;
    WorkerNetAddress mAddress;
    Long mLeaseId = -1L;

    public WorkerService(BlockWorkerInfo workerInfo) {
      super(workerInfo.getNetAddress().dumpMainInfo(), null);
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("WorkerId", mWorkerId.get())
          .add("WorkerAddr", mAddress.toString())
          .add("LeaseId", mLeaseId)
          .toString();
    }
  }

  @Override
  public void heartbeat(long timeLimitMs) throws InterruptedException {
//    KV kvClient = mEtcdClient.getEtcdClient().getKVClient();
    ByteSequence key = ByteSequence.from("test_key".getBytes());
    ByteSequence value = ByteSequence.from("test_value".getBytes());

// put the key-value
//    kvClient.put(key, value, PutOption.newBuilder().withLeaseId()).get();
//
//// get the CompletableFuture
//    CompletableFuture<GetResponse> getFuture = kvClient.get(key);
//
//// get the value from CompletableFuture
//    GetResponse response = getFuture.get();
//
//// delete the key
//    kvClient.delete(key).get();
  }

  @Override
  public void close() {

  }
}
