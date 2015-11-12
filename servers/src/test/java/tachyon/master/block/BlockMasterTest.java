/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.block;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tachyon.exception.TachyonException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.collections.IndexedSet;
import tachyon.master.block.meta.MasterBlockInfo;
import tachyon.master.block.meta.MasterWorkerInfo;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;

/**
 * Unit tests for tachyon.master.block.BlockMaster.
 */
public class BlockMasterTest {

  private BlockMaster mMaster;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_LOST_WORKER_DETECTION,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mMaster = new BlockMaster(blockJournal);
    mMaster.start(true);
  }

  @Test
  public void countBytesTest() throws Exception {
    Assert.assertEquals(0L, mMaster.getCapacityBytes());
    Assert.assertEquals(0L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of(), mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of(), mMaster.getUsedBytesOnTiers());
    long worker1 = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    long worker2 = mMaster.getWorkerId(new NetAddress("localhost", 82, 83));
    addWorker(mMaster, worker1, Arrays.asList("MEM", "SSD", "HDD"),
        ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD", 30L),
        ImmutableMap.of("MEM", 20L, "SSD", 50L, "HDD", 10L));
    Assert.assertEquals(330L, mMaster.getCapacityBytes());
    Assert.assertEquals(80L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD", 30L),
        mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of("MEM", 20L, "SSD", 50L, "HDD", 10L),
        mMaster.getUsedBytesOnTiers());
    addWorker(mMaster, worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 500L),
        ImmutableMap.of("MEM", 300L));
    Assert.assertEquals(830L, mMaster.getCapacityBytes());
    Assert.assertEquals(380L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of("MEM", 600L, "SSD", 200L, "HDD", 30L),
        mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of("MEM", 320L, "SSD", 50L, "HDD", 10L),
        mMaster.getUsedBytesOnTiers());
  }

  @Test
  public void getLostWorkersInfoTest() throws Exception {
    MasterWorkerInfo workerInfo1 = new MasterWorkerInfo(1, new NetAddress("localhost", 80, 81));
    MasterWorkerInfo workerInfo2 = new MasterWorkerInfo(2, new NetAddress("localhost", 82, 83));
    IndexedSet<MasterWorkerInfo> lostWorkers = Whitebox.getInternalState(mMaster, "mLostWorkers");
    lostWorkers.add(workerInfo1);
    Assert.assertEquals(ImmutableSet.of(workerInfo1.generateClientWorkerInfo()),
        mMaster.getLostWorkersInfo());
    lostWorkers.add(workerInfo2);

    final Set<WorkerInfo> expected = ImmutableSet.of(workerInfo1.generateClientWorkerInfo(),
        workerInfo2.generateClientWorkerInfo());

    Assert.assertEquals(expected, mMaster.getLostWorkersInfo());
  }

  @Test
  public void registerLostWorkerTest() throws Exception {
    final NetAddress na = new NetAddress("localhost", 80, 81);
    final long expectedId = 1;
    final MasterWorkerInfo workerInfo1 = new MasterWorkerInfo(expectedId, na);
    IndexedSet<MasterWorkerInfo> lostWorkers = Whitebox.getInternalState(mMaster, "mLostWorkers");

    workerInfo1.addBlock(1L);
    lostWorkers.add(workerInfo1);
    final long workerId = mMaster.getWorkerId(na);
    Assert.assertEquals(expectedId, workerId);

    final List<Long> blocks = ImmutableList.of(42L);
    mMaster.workerRegister(workerId, Arrays.asList("MEM"), ImmutableMap.of("MEM", 1024L),
        ImmutableMap.of("MEM", 1024L), ImmutableMap.of("MEM", blocks));

    final Set<Long> expectedBlocks = ImmutableSet.of(42L);
    final Set<Long> actualBlocks = workerInfo1.getBlocks();

    Assert.assertEquals("The master should reflect the blocks declared at registration",
        expectedBlocks, actualBlocks);
  }

  @Test
  public void removeBlocksTest() throws Exception {
    long worker1 = mMaster.getWorkerId(new NetAddress("test1", 1, 2));
    long worker2 = mMaster.getWorkerId(new NetAddress("test2", 1, 2));
    List<Long> workerBlocks = Arrays.asList(1L, 2L, 3L);
    HashMap<String, List<Long>> noBlocksInTiers = Maps.newHashMap();
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), noBlocksInTiers);
    mMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), noBlocksInTiers);
    mMaster.commitBlock(worker1, 1L, "MEM", 1L, 1L);
    mMaster.commitBlock(worker1, 2L, "MEM", 2L, 1L);
    mMaster.commitBlock(worker1, 3L, "MEM", 3L, 1L);
    mMaster.commitBlock(worker2, 1L, "MEM", 1L, 1L);
    mMaster.commitBlock(worker2, 2L, "MEM", 2L, 1L);
    mMaster.commitBlock(worker2, 3L, "MEM", 3L, 1L);
    mMaster.removeBlocks(workerBlocks);
  }

  @Test
  public void workerHeartbeatTest() throws Exception {
    long workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(mMaster, "mWorkers");
    IndexedSet.FieldIndex<MasterWorkerInfo> idIdx = Whitebox.getInternalState(mMaster, "mIdIndex");

    MasterWorkerInfo workerInfo = workers.getFirstByField(idIdx, workerId);
    final Map<String, Long> USED_BYTES_ON_TIERS = ImmutableMap.of("MEM", 125L);
    final List<Long> INITIAL_BLOCKS = ImmutableList.of(1L, 2L);
    addWorker(mMaster, workerId, Arrays.asList("MEM"), ImmutableMap.of("MEM", 500L),
        USED_BYTES_ON_TIERS);
    for (Long block : INITIAL_BLOCKS) {
      mMaster.commitBlock(workerId, USED_BYTES_ON_TIERS.get("MEM"), "MEM", block, 100L);
    }

    // test heartbeat removing a block
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    final long REMOVED_BLOCK = INITIAL_BLOCKS.get(0);
    Command heartBeat1 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.of(REMOVED_BLOCK), ImmutableMap.<String, List<Long>>of());
    Set<Long> expectedBlocks =
        Sets.difference(ImmutableSet.copyOf(INITIAL_BLOCKS), ImmutableSet.of(REMOVED_BLOCK));
    // block is removed from worker info
    Assert.assertEquals(expectedBlocks, workerInfo.getBlocks());
    // worker is removed from block info
    Map<Long, MasterBlockInfo> blocks = Whitebox.getInternalState(mMaster, "mBlocks");
    Assert.assertEquals(ImmutableSet.of(), blocks.get(REMOVED_BLOCK).getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat1);

    // test heartbeat adding back the block
    List<Long> readdedBlocks = ImmutableList.of(REMOVED_BLOCK);
    Command heartBeat2 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.of("MEM", readdedBlocks));
    // block is restored to worker info
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    // worker is restored to block info
    Assert.assertEquals(ImmutableSet.of(workerId), blocks.get(REMOVED_BLOCK).getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);

    // test heartbeat where the master tells the worker to remove a block
    final long BLOCK_TO_FREE = INITIAL_BLOCKS.get(1);
    workerInfo.updateToRemovedBlock(true, BLOCK_TO_FREE);
    Command heartBeat3 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.<String, List<Long>>of());
    Assert.assertEquals(new Command(CommandType.Free, ImmutableList.<Long>of(BLOCK_TO_FREE)),
        heartBeat3);
  }

  @Test
  public void heartbeatStatusTest() throws Exception {
    long workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(mMaster, "mWorkers");
    IndexedSet.FieldIndex<MasterWorkerInfo> idIdx = Whitebox.getInternalState(mMaster, "mIdIndex");

    MasterWorkerInfo workerInfo = workers.getFirstByField(idIdx, workerId);
    final Map<String, Long> INITIAL_USED_BYTES_ON_TIERS =
        ImmutableMap.of("MEM", 25L, "SSD", 50L, "HDD", 125L);
    addWorker(mMaster, workerId, Arrays.asList("MEM", "SSD", "HDD"),
        ImmutableMap.of("MEM", 50L, "SSD", 100L, "HDD", 500L), INITIAL_USED_BYTES_ON_TIERS);

    long lastUpdatedTime1 = workerInfo.getLastUpdatedTimeMs();
    Thread.sleep(1); // sleep for 1ms so that lastUpdatedTimeMs is guaranteed to change
    final Map<String, Long> NEW_USED_BYTES_ON_TIERS =
        ImmutableMap.of("MEM", 50L, "SSD", 100L, "HDD", 500L);
    // test simple heartbeat letting the master know that more bytes are being used
    Command heartBeat = mMaster.workerHeartbeat(workerId, NEW_USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.<String, List<Long>>of());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat);
    // updates the number of used bytes on the worker
    Assert.assertEquals(NEW_USED_BYTES_ON_TIERS, workerInfo.getUsedBytesOnTiers());
    // updates the workers last updated time
    Assert.assertNotEquals(lastUpdatedTime1, workerInfo.getLastUpdatedTimeMs());
  }

  @Test
  public void unknownHeartbeatTest() throws Exception {
    Command heartBeat = mMaster.workerHeartbeat(0, null, null, null);
    Assert.assertEquals(new Command(CommandType.Register, ImmutableList.<Long>of()), heartBeat);
  }

  @Test
  public void detectLostWorkerTest() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 5, TimeUnit.SECONDS);
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(mMaster, "mWorkers");
    IndexedSet.FieldIndex<MasterWorkerInfo> idIdx = Whitebox.getInternalState(mMaster, "mIdIndex");

    // Get a new worker id.
    long workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    MasterWorkerInfo workerInfo = workers.getFirstByField(idIdx, workerId);
    Assert.assertNotNull(workerInfo);

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // No workers should be considered lost.
    Assert.assertEquals(0, mMaster.getLostWorkersInfo().size());
    Assert.assertNotNull(workers.getFirstByField(idIdx, workerId));

    // Set the last updated time for the worker to be definitely too old, so it is considered lost.
    Whitebox.setInternalState(workerInfo, "mLastUpdatedTimeMs", 0);

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // There should be a lost worker.
    Assert.assertEquals(1, mMaster.getLostWorkersInfo().size());
    Assert.assertNull(workers.getFirstByField(idIdx, workerId));

    // Get the worker id again, simulating the lost worker re-registering.
    workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    Assert.assertNotNull(workers.getFirstByField(idIdx, workerId));

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // No workers should be considered lost.
    Assert.assertEquals(0, mMaster.getLostWorkersInfo().size());
    Assert.assertNotNull(workers.getFirstByField(idIdx, workerId));
  }

  private void addWorker(BlockMaster master, long workerId, List<String> storageTierAliases,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers)
          throws TachyonException {
    master.workerRegister(workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
        Maps.<String, List<Long>>newHashMap());
  }
}
