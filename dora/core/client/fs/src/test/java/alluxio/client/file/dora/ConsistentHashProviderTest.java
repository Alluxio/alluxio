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

import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerIdentityTestUtils;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    ConsistentHashProvider provider = new ConsistentHashProvider(
        1, WORKER_LIST_TTL_MS, NUM_VIRTUAL_NODES);
    assertThrows(IllegalStateException.class, () -> provider.get(OBJECT_KEY, 0));
  }

  @Test
  /**
   * This test calculates the standard deviation over mean on the collection of
   * virtual nodes assigned to physical nodes. It arbitrarily bounds it at 0.25,
   * but ideally this number should get smaller over time as we improve hashing algorithm
   * and use better ways to assign virtual nodes to physical nodes.
   *
   * This uses 2000 virtual nodes and 50 physical nodes, if these parameters change,
   * the bound is likely going to change.
   */
  public void virtualNodeDistribution() {
    ConsistentHashProvider provider = new ConsistentHashProvider(
        1, WORKER_LIST_TTL_MS, NUM_VIRTUAL_NODES);
    Set<WorkerIdentity> workerList = generateRandomWorkerList(50);
    // set initial state
    provider.refresh(workerList);
    NavigableMap<Integer, WorkerIdentity> map = provider.getActiveNodesMap();
    Map<WorkerIdentity, Long> count = new HashMap<>();
    long last = Integer.MIN_VALUE;
    for (Map.Entry<Integer, WorkerIdentity> entry : map.entrySet()) {
      count.put(entry.getValue(), count.getOrDefault(entry.getValue(), 0L)
          + (entry.getKey() - last));
      last = entry.getKey().intValue();
    }
    assertTrue(calcSDoverMean(count.values()) < 0.25);
  }

  private double calcSDoverMean(Collection<Long> list) {
    long sum = 0L;
    double var = 0;
    for (long num : list) {
      sum += num;
    }
    double avg = sum * 1.0 / list.size();
    for (long num : list) {
      var = var + (num - avg) * (num - avg);
    }
    return Math.sqrt(var / list.size()) / avg;
  }

  @Test
  public void concurrentInitialization() {
    ConsistentHashProvider provider = new ConsistentHashProvider(
        1, WORKER_LIST_TTL_MS, NUM_VIRTUAL_NODES);
    final int numThreads = 16;
    CountDownLatch startSignal = new CountDownLatch(numThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Set<WorkerIdentity>> lists = IntStream.range(0, numThreads)
        .mapToObj(i -> generateRandomWorkerList(5))
        .collect(Collectors.toList());
    List<Future<NavigableMap<Integer, WorkerIdentity>>> futures = IntStream.range(0, numThreads)
        .mapToObj(i -> {
          Set<WorkerIdentity> list = lists.get(i);
          return executorService.submit(() -> {
            startSignal.countDown();
            try {
              startSignal.await();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
            provider.refresh(list);
            return provider.getActiveNodesMap();
          });
        })
        .collect(Collectors.toList());
    Set<NavigableMap<Integer, WorkerIdentity>> mapSet = futures.stream().map(future -> {
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
    Set<WorkerIdentity> workerInfoListUsedByPolicy = provider.getLastWorkers();
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
      ConsistentHashProvider provider = new ConsistentHashProvider(
          1, WORKER_LIST_TTL_MS, NUM_VIRTUAL_NODES);
      provider.refresh(generateRandomWorkerList(50));
      long initialCount = provider.getUpdateCount();
      Thread.sleep(WORKER_LIST_TTL_MS);

      CountDownLatch startSignal = new CountDownLatch(numThreads);

      // generate a list of distinct maps for each thread
      List<Set<WorkerIdentity>> listsPerThread = IntStream.range(0, numThreads)
          .mapToObj(i -> generateRandomWorkerList(50))
          .collect(Collectors.toList());
      List<Future<?>> futures = IntStream.range(0, numThreads)
          .mapToObj(i -> {
            Set<WorkerIdentity> list = listsPerThread.get(i);
            return executorService.submit(() -> {
              startSignal.countDown();
              try {
                startSignal.await();
              } catch (InterruptedException e) {
                fail("interrupted");
              }
              provider.refresh(list);
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
      Set<WorkerIdentity> workerInfoListUsedByPolicy = provider.getLastWorkers();
      assertTrue(listsPerThread.contains(workerInfoListUsedByPolicy));
      assertEquals(
          ConsistentHashProvider.build(workerInfoListUsedByPolicy, NUM_VIRTUAL_NODES),
          provider.getActiveNodesMap());
    }
  }

  @Test
  public void workerListTtl() throws Exception {
    ConsistentHashProvider provider = new ConsistentHashProvider(
        1, WORKER_LIST_TTL_MS, NUM_VIRTUAL_NODES);
    Set<WorkerIdentity> workerList = generateRandomWorkerList(5);
    // set initial state
    provider.refresh(workerList);
    long initialUpdateCount = provider.getUpdateCount();
    assertEquals(workerList, provider.getLastWorkers());
    assertEquals(
        ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
        provider.getActiveNodesMap());

    // before TTL is up, refresh does not change the internal states of the provider
    Set<WorkerIdentity> newList = generateRandomWorkerList(5);
    provider.refresh(newList);
    assertEquals(0, provider.getUpdateCount() - initialUpdateCount);
    assertNotEquals(newList, workerList);
    assertEquals(workerList, provider.getLastWorkers());
    assertEquals(
        ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
        provider.getActiveNodesMap());

    // after TTL expires, refresh should change the worker list and the active nodes map
    Thread.sleep(WORKER_LIST_TTL_MS);
    provider.refresh(newList);
    assertEquals(1, provider.getUpdateCount() - initialUpdateCount);
    assertEquals(newList, provider.getLastWorkers());
    assertEquals(
        ConsistentHashProvider.build(newList, NUM_VIRTUAL_NODES),
        provider.getActiveNodesMap());
  }

  private Set<WorkerIdentity> generateRandomWorkerList(int count) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ImmutableSet.Builder<WorkerIdentity> builder = ImmutableSet.builder();
    while (count-- > 0) {
      WorkerIdentity id = WorkerIdentityTestUtils.randomLegacyId();
      builder.add(id);
    }
    return builder.build();
  }
}
