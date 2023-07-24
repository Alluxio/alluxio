/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.membership;

import alluxio.annotation.SuppressFBWarnings;

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

/**
 * DistributedBarrierRecipe for etcd. (WIP)
 */
public class BarrierRecipe {
  private static final Logger LOG = LoggerFactory.getLogger(BarrierRecipe.class);
  Client mClient;
  String mClusterIdentifier;
  long mLeaseTtlInSec = 2L;
  String mBarrierPath;
  String mNewBarrierPath = "/new-barrier";
  CountDownLatch mLatch = new CountDownLatch(1);

  /**
   * CTOR for BarrierRecipe.
   * @param client
   * @param barrierPath
   * @param clusterIdentifier
   * @param leaseTtlSec
   */
  public BarrierRecipe(AlluxioEtcdClient client, String barrierPath,
                       String clusterIdentifier, long leaseTtlSec) {
    client.connect();
    mClient = client.getEtcdClient();
    mClusterIdentifier = clusterIdentifier;
    mLeaseTtlInSec = leaseTtlSec;
    mBarrierPath = barrierPath;
  }

  /**
   * Set the barrier, create the corresponding kv pair on etcd.
   * @throws IOException
   */
  public void setBarrier() throws IOException {
    try {
      Txn txn = mClient.getKVClient().txn();
      ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(
              new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0L)))
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

  /**
   * Remove the barrier path.
   * @throws IOException
   */
  public void removeBarrier() throws IOException {
    try {
      GetResponse getResp = mClient.getKVClient().get(
          ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8)).get();
      LOG.info("get key:{}, [{}]", mBarrierPath, getResp.getKvs());
      Txn txn = mClient.getKVClient().txn();
      ByteSequence key = ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8);
      ByteSequence key1 = ByteSequence.from(mNewBarrierPath, StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(
              new Cmp(key, Cmp.Op.GREATER, CmpTarget.createRevision(0L)))
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

  /**
   * Wait on barrier, waiting for the path to get deleted.
   */
  public void waitOnBarrierInternal() {
    try {
      Watch.Watcher watcher = mClient.getWatchClient().watch(
          ByteSequence.EMPTY, WatchOption.newBuilder().build(), new Watch.Listener() {
            @Override
            public void onNext(WatchResponse response) {
              WatchEvent event = response.getEvents().get(0);
            }

            @Override
            public void onError(Throwable throwable) {
              // NOOP
            }

            @Override
            public void onCompleted() {
              // NOOP
            }
          });
      mClient.getWatchClient().watch(ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8),
          WatchOption.DEFAULT, watchResponse -> {
            for (WatchEvent event : watchResponse.getEvents()) {
              if (event.getEventType() == WatchEvent.EventType.DELETE
                  && event.getKeyValue().getKey().equals(
                      ByteSequence.from(mBarrierPath, StandardCharsets.UTF_8))) {
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

  /**
   * Wait on barrier with no time restraint.
   * @throws InterruptedException
   */
  public void waitOnBarrier() throws InterruptedException {
    waitOnBarrierInternal();
    mLatch.await();
  }

  /**
   * Wait on barrier with a given timeout.
   * @param time
   * @param timeUnit
   * @throws InterruptedException
   */
  @SuppressFBWarnings({"RV_RETURN_VALUE_IGNORED"})
  public void waitOnBarrier(long time, TimeUnit timeUnit) throws InterruptedException {
    waitOnBarrierInternal();
    mLatch.await(time, timeUnit);
  }

  /**
   * TEMPORARY simple barrier test - WIP.
   * @param alluxioEtcdClient
   */
  public static void testBarrier(AlluxioEtcdClient alluxioEtcdClient) {
    try {
      BarrierRecipe barrierRecipe = new BarrierRecipe(alluxioEtcdClient, "/barrier-test",
          "cluster1", 2L);
      LOG.info("Setting barrier.");
      barrierRecipe.setBarrier();
      Thread t = new Thread(() -> {
        try {
          LOG.info("start waiting on barrier...");
          barrierRecipe.waitOnBarrier();
          LOG.info("wait on barrier done.");
        } catch (InterruptedException e) {
          LOG.info("wait on barrier ex:", e);
          throw new RuntimeException(e);
        }
      });
      t.start();
      Thread.sleep(3000);
      LOG.info("Removing barrier.");
      barrierRecipe.removeBarrier();
      t.join();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
