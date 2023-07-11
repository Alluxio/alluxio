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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.dora.WorkerLocationPolicy.ConsistentHashProvider;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
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

public class WorkerLocationPolicyTest {
  private static final long WORKER_LIST_TTL_MS = 20;
  private static final String OBJECT_KEY = "/path/to/object";
  private static final int NUM_VIRTUAL_NODES = 100;

  @Test
  public void uninitializedThrowsException() {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    Assert.assertThrows(IllegalStateException.class, () -> provider.get(OBJECT_KEY, 0));
  }

  @Test
  public void concurrentInitialization() {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    final int numThreads = Runtime.getRuntime().availableProcessors();
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
  // todo(bowen): this test can be flaky if the test subject is not thread safe
  public void concurrentRefresh() throws Exception {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    provider.refresh(generateRandomWorkerList(5), NUM_VIRTUAL_NODES);
    NavigableMap<Integer, BlockWorkerInfo> initialMap = provider.getActiveNodesMap();
    assertNotNull(initialMap);
    Thread.sleep(WORKER_LIST_TTL_MS);

    final int numThreads = Runtime.getRuntime().availableProcessors();
    CountDownLatch startSignal = new CountDownLatch(numThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    // generate a list of distinct maps for each thread
    List<List<BlockWorkerInfo>> listsPerThread = IntStream.range(0, numThreads)
        .mapToObj(i -> generateRandomWorkerList(5))
        .collect(Collectors.toList());
    List<Future<NavigableMap<Integer, BlockWorkerInfo>>> futures = IntStream.range(0, numThreads)
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
            return provider.getActiveNodesMap();
          });
        })
        .collect(Collectors.toList());
    int numOfThreadsWhichUpdatedMap = 0;
    Set<NavigableMap<Integer, BlockWorkerInfo>> distinctMaps = new HashSet<>();
    for (int i = 0; i < futures.size(); i++) {
      final NavigableMap<Integer, BlockWorkerInfo> map;
      try {
        map = futures.get(i).get();
      } catch (InterruptedException interruptedException) {
        throw new AssertionError("interrupted", interruptedException);
      } catch (ExecutionException e) {
        throw new AssertionError("failed to run thread", e);
      }
      distinctMaps.add(map);
      // the map returned by the i-th thread can be one of the 3 cases:
      // 1. the refresh call returns before the writer thread could finish updating the map,
      //    so the map is the same as the initial map
      // 2. this thread is the one that gets to update the map, so the map is the i-th
      //    map in map lists
      // 3. the refresh call executes after the writer finishes updating the map, so this thread
      //    sees the up-to-date map set by the writer, which is not the i-th map
      // in summary, if only one thread gets to update the map, then case 2 can occur only
      // once.

      if (initialMap.equals(map)) { // case 1
        numOfThreadsWhichUpdatedMap += 0;
      } else if (ConsistentHashProvider.build(
          listsPerThread.get(i), NUM_VIRTUAL_NODES).equals(map)) { // case 2
        numOfThreadsWhichUpdatedMap += 1;
      } else { // case 3
        numOfThreadsWhichUpdatedMap += 0;
      }
    }

    assertTrue("at most two possible outcomes about the active nodes map should be visible, "
        + "but got " + distinctMaps.size(),
        distinctMaps.size() >= 1 && distinctMaps.size() <= 2);
    // check only one thread updated the worker map
    assertEquals(1, numOfThreadsWhichUpdatedMap);
  }

  @Test
  public void workerListTtl() throws Exception {
    ConsistentHashProvider provider = new ConsistentHashProvider(1, WORKER_LIST_TTL_MS);
    List<BlockWorkerInfo> workerList = generateRandomWorkerList(5);
    // set initial state
    provider.refresh(workerList, NUM_VIRTUAL_NODES);
    assertEquals(workerList, provider.getLastWorkerInfos());
    assertEquals(
        ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
        provider.getActiveNodesMap());

    // before TTL is up, refresh does not change the internal states of the provider
    List<BlockWorkerInfo> newList = generateRandomWorkerList(5);
    provider.refresh(newList, NUM_VIRTUAL_NODES);
    assertNotEquals(newList, workerList);
    assertEquals(workerList, provider.getLastWorkerInfos());
    assertEquals(
        ConsistentHashProvider.build(workerList, NUM_VIRTUAL_NODES),
        provider.getActiveNodesMap());

    // after TTL expires, refresh should change the worker list and the active nodes map
    Thread.sleep(WORKER_LIST_TTL_MS);
    provider.refresh(newList, NUM_VIRTUAL_NODES);
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
