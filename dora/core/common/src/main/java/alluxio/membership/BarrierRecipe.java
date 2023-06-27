package alluxio.membership;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BarrierRecipe {
  private static final Logger LOG = LoggerFactory.getLogger(BarrierRecipe.class);
  Client mClient;
  String mClusterIdentifier;
  long mLeaseTtlInSec = 2L;
  String mBarrierPath;
  String mNewBarrierPath = "/new-barrier";
  CountDownLatch mLatch = new CountDownLatch(1);
  public BarrierRecipe(AlluxioEtcdClient client, String barrierPath, String clusterIdentifier, long leaseTtlSec) {
    client.connect();
    mClient = client.getEtcdClient();
    mClusterIdentifier = clusterIdentifier;
    mLeaseTtlInSec = leaseTtlSec;
    mBarrierPath = barrierPath;
  }

  public void setBarrier() throws IOException {
    try {
      Txn txn = mClient.getKVClient().txn();
      ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0L)))
          .Then(Op.put(key, ByteSequence.EMPTY, PutOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      if (!txnResponse.isSucceeded()) {
        throw new IOException("Failed to set barrier for path:" + mBarrierPath);
      }
      LOG.info("Successfully set barrier:{}", mBarrierPath);
    } catch (ExecutionException | InterruptedException ex) {
      LOG.error("Exception during setBarrier.", ex);
    }
  }

  public void removeBarrier() throws IOException {
    try {
      GetResponse getResp = mClient.getKVClient().get(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8)).get();
      LOG.info("get key:{}, [{}]", mBarrierPath, getResp.getKvs());
      Txn txn = mClient.getKVClient().txn();
      ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
      ByteSequence key1 = ByteSequence.from(mNewBarrierPath, StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.createRevision(0L)))
          .Then(Op.delete(key, DeleteOption.DEFAULT))
          .Then(Op.put(key1, ByteSequence.EMPTY, PutOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      if (!txnResponse.isSucceeded()) {
        throw new IOException("Failed to remove barrier for path:" + mBarrierPath);
      }
      LOG.info("Successfully remove barrier:{}", mBarrierPath);
    } catch (ExecutionException | InterruptedException ex) {
      LOG.error("Exception during removeBarrier.", ex);
    }
  }

  public void waitOnBarrierInternal() {
    try {
      Watch.Watcher watcher = mClient.getWatchClient().watch(ByteSequence.EMPTY, WatchOption.newBuilder().build(), new Watch.Listener() {
        @Override
        public void onNext(WatchResponse response) {
          WatchEvent event = response.getEvents().get(0);
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
      });
      mClient.getWatchClient().watch(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8),
          WatchOption.DEFAULT, watchResponse -> {
            for (WatchEvent event : watchResponse.getEvents()) {
              if (event.getEventType() == WatchEvent.EventType.DELETE &&
                  event.getKeyValue().getKey().equals(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8))) {
                LOG.info("Delete event observed on path {}", mBarrierPath);
                mLatch.countDown();
              }
            }
          });
      mLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Barrier wait done.");
  }

  // wait forever
  public void waitOnBarrier() throws InterruptedException {
    waitOnBarrierInternal();
    mLatch.await();
  }

  public void waitOnBarrier(long time, TimeUnit timeUnit) throws InterruptedException {
    waitOnBarrierInternal();
    mLatch.await(time, timeUnit);
  }
}
