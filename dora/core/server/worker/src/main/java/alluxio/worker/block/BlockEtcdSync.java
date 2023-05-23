package alluxio.worker.block;

import alluxio.heartbeat.HeartbeatExecutor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.options.PutOption;

public class BlockEtcdSync implements HeartbeatExecutor {
  EtcdClient mEtcdClient;


  public BlockEtcdSync() {
    mEtcdClient = new EtcdClient();
    mEtcdClient.connect();
  }


  @Override
  public void heartbeat() throws InterruptedException {
    KV kvClient = mEtcdClient.getEtcdClient().getKVClient();
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
