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

package alluxio.client.file.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsistentHashProviderTest {
  private static final long WORKER_LIST_TTL_MS = 20;
  private static final String OBJECT_KEY = "/path/to/object";
  private static final int NUM_VIRTUAL_NODES = 100;

  @Test
  public void uninitializedThrowsException() {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    assertThrows(IllegalStateException.class, () -> provider.get(OBJECT_KEY, 0));
  }

  @Test
  public void concurrentInitialization() {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    final int numThreads = 16;
    CountDownLatch startSignal = new CountDownLatch(numThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<List<BlockWorkerInfo>> lists = IntStream.range(0, numThreads)
            .mapToObj(i -> generateRandomWorkerList(5))
            .collect(Collectors.toList());
    List<Future<NavigableMap<Integer, BlockWorkerInfo>>> futures = IntStream.range(0, numThreads)
            .mapToObj(i -> {
              List<BlockWorkerInfo> list = lists.get(i);
              return executorService.submit(() -> {
                startSignal.countDown();
                try {
                  startSignal.await();
                } catch (InterruptedException e) {
                  fail("interrupted");
                }
                provider.refresh(list, NUM_VIRTUAL_NODES);
                return provider.getActiveNodesMap();
              });
            })
            .collect(Collectors.toList());
    Set<NavigableMap<Integer, BlockWorkerInfo>> mapSet = futures.stream().map(future -> {
      try {
        return future.get();
      } catch (InterruptedException interruptedException) {
        throw new AssertionError("interrupted", interruptedException);
      } catch (ExecutionException e) {
        throw new AssertionError("failed to run thread", e);
      }
    }).collect(Collectors.toSet());

    assertEquals(1, mapSet.size());
    // check if the worker list is one of the lists provided by the threads
    List<BlockWorkerInfo> workerInfoListUsedByPolicy = provider.getLastWorkerInfos();
    assertTrue(lists.contains(workerInfoListUsedByPolicy));
    assertEquals(
            ConsistentHashProvider.build(workerInfoListUsedByPolicy, NUM_VIRTUAL_NODES),
            provider.getActiveNodesMap());
  }

  @Test
  // Notes on thread safety:
  // This test tries to ensure the test subject live up to its thread safety guarantees.
  // 1. When the test subject is actually correctly implemented, this test will *always* pass.
  // 2. When it is not, this test cannot reliably detect that, since it's a matter of
  //    chance that race conditions manifest themselves. This test may appear to be flaky,
  //    but chances are that there are thread safety issues with the test subject.
  // To decrease the chance of false negatives, you can run this test manually multiple times
  // until you are confident it's free of race conditions.
  public void concurrentRefresh() throws Exception {
    final int numThreads = 16;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    for (int repeat = 0; repeat < 100; repeat++) {
      ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
      provider.refresh(generateRandomWorkerList(50), NUM_VIRTUAL_NODES);
      long initialCount = provider.getUpdateCount();
      Thread.sleep(WORKER_LIST_TTL_MS);

      CountDownLatch startSignal = new CountDownLatch(numThreads);

      // generate a list of distinct maps for each thread
      List<List<BlockWorkerInfo>> listsPerThread = IntStream.range(0, numThreads)
              .mapToObj(i -> generateRandomWorkerList(50))
              .collect(Collectors.toList());
      List<Future<?>> futures = IntStream.range(0, numThreads)
              .mapToObj(i -> {
                List<BlockWorkerInfo> list = listsPerThread.get(i);
                return executorService.submit(() -> {
                  startSignal.countDown();
                  try {
                    startSignal.await();
                  } catch (InterruptedException e) {
                    fail("interrupted");
                  }
                  provider.refresh(list, NUM_VIRTUAL_NODES);
                });
              })
              .collect(Collectors.toList());
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException interruptedException) {
          throw new AssertionError("interrupted", interruptedException);
        } catch (ExecutionException e) {
          throw new AssertionError("failed to run thread", e);
        }
      }
      // only one thread actually updated the map
      assertEquals(1, provider.getUpdateCount() - initialCount);
      // check if the worker list is one of the lists provided by the threads
      List<BlockWorkerInfo> workerInfoListUsedByPolicy = provider.getLastWorkerInfos();
      assertTrue(listsPerThread.contains(workerInfoListUsedByPolicy));
      assertEquals(
              ConsistentHashProvider.build(workerInfoListUsedByPolicy, NUM_VIRTUAL_NODES),
              provider.getActiveNodesMap());
    }
  }

  @Test
  public void workerListTtl() throws Exception {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    List<BlockWorkerInfo> workerList = generateRandomWorkerList(5);
    // set initial state
    provider.refresh(workerList, NUM_VIRTUAL_NODES);
    long initialUpdateCount = provider.getUpdateCount();
    assertEquals(workerList, provider.getLastWorkerInfos());
    assertEquals(
            ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
            provider.getActiveNodesMap());

    // before TTL is up, refresh does not change the internal states of the provider
    List<BlockWorkerInfo> newList = generateRandomWorkerList(5);
    provider.refresh(newList, NUM_VIRTUAL_NODES);
    assertEquals(0, provider.getUpdateCount() - initialUpdateCount);
    assertNotEquals(newList, workerList);
    assertEquals(workerList, provider.getLastWorkerInfos());
    assertEquals(
            ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
            provider.getActiveNodesMap());

    // after TTL expires, refresh should change the worker list and the active nodes map
    Thread.sleep(WORKER_LIST_TTL_MS);
    provider.refresh(newList, NUM_VIRTUAL_NODES);
    assertEquals(1, provider.getUpdateCount() - initialUpdateCount);
    assertEquals(newList, provider.getLastWorkerInfos());
    assertEquals(
            ConsistentHashProvider.build(newList, NUM_VIRTUAL_NODES),
            provider.getActiveNodesMap());
  }

  private List<BlockWorkerInfo> generateRandomWorkerList(int count) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ImmutableList.Builder<BlockWorkerInfo> builder = ImmutableList.builder();
    while (count-- > 0) {
      WorkerNetAddress netAddress = new WorkerNetAddress();
      netAddress.setHost(RandomStringUtils.randomAlphanumeric(10));
      netAddress.setContainerHost(RandomStringUtils.randomAlphanumeric(10));
      netAddress.setDomainSocketPath(RandomStringUtils.randomAlphanumeric(10));
      netAddress.setRpcPort(rng.nextInt(0, 65536));
      netAddress.setDataPort(rng.nextInt(0, 65536));
      netAddress.setNettyDataPort(rng.nextInt(0, 65536));
      netAddress.setSecureRpcPort(rng.nextInt(0, 65536));
      netAddress.setWebPort(rng.nextInt(0, 65536));
      netAddress.setTieredIdentity(
              new TieredIdentity(ImmutableList.of(new TieredIdentity.LocalityTier("tier", "loc"))));

      BlockWorkerInfo workerInfo = new BlockWorkerInfo(netAddress,
              rng.nextLong(0, Constants.GB), rng.nextLong(0, Constants.GB));
      builder.add(workerInfo);
    }
    return builder.build();
  }
}