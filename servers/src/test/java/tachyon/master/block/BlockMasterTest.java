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
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tachyon.StorageLevelAlias;
import tachyon.master.block.meta.MasterWorkerInfo;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.test.Tester;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.master.block.BlockMaster.
 */
public class BlockMasterTest implements Tester<BlockMaster> {

  private BlockMaster.PrivateAccess mPrivateAccess;
  private BlockMaster mMaster;

  @Override
  public void receiveAccess(Object access) {
    mPrivateAccess = (BlockMaster.PrivateAccess) access;
  }

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void initialize() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mMaster = new BlockMaster(blockJournal);
    mMaster.grantAccess(BlockMasterTest.this); // initializes mPrivateAccess
  }

  @Test
  public void countBytesTest() throws Exception {
    Assert.assertEquals(0L, mMaster.getCapacityBytes());
    Assert.assertEquals(0L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableList.of(0L, 0L, 0L), mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableList.of(0L, 0L, 0L), mMaster.getUsedBytesOnTiers());
    long worker1 = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    long worker2 = mMaster.getWorkerId(new NetAddress("localhost", 82, 83));
    addWorker(mMaster, worker1, ImmutableList.of(100L, 200L, 30L), ImmutableList.of(20L, 50L, 10L));
    Assert.assertEquals(330L, mMaster.getCapacityBytes());
    Assert.assertEquals(80L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableList.of(100L, 200L, 30L), mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableList.of(20L, 50L, 10L), mMaster.getUsedBytesOnTiers());
    addWorker(mMaster, worker2, ImmutableList.of(500L), ImmutableList.of(300L));
    Assert.assertEquals(830L, mMaster.getCapacityBytes());
    Assert.assertEquals(380L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableList.of(600L, 200L, 30L), mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableList.of(320L, 50L, 10L), mMaster.getUsedBytesOnTiers());
  }

  @Test
  public void getLostWorkersInfoTest() throws Exception {
    MasterWorkerInfo workerInfo1 = new MasterWorkerInfo(1, new NetAddress("localhost", 80, 81));
    MasterWorkerInfo workerInfo2 = new MasterWorkerInfo(2, new NetAddress("localhost", 82, 83));
    mPrivateAccess.addLostWorker(workerInfo1);
    Assert.assertEquals(ImmutableList.of(workerInfo1.generateClientWorkerInfo()),
        mMaster.getLostWorkersInfo());
    mPrivateAccess.addLostWorker(workerInfo2);
    Assert.assertEquals(
        ImmutableList.of(workerInfo1.generateClientWorkerInfo(),
            workerInfo2.generateClientWorkerInfo()), mMaster.getLostWorkersInfo());
  }

  @Test
  public void removeBlocksTest() throws Exception {
    mMaster.start(true);
    long worker1 = mMaster.getWorkerId(new NetAddress("test1", 1, 2));
    long worker2 = mMaster.getWorkerId(new NetAddress("test2", 1, 2));
    List<Long> workerBlocks = Arrays.asList(1L, 2L, 3L);
    HashMap<Long, List<Long>> noBlocksInTiers = Maps.newHashMap();
    mMaster.workerRegister(worker1, Arrays.asList(100L), Arrays.asList(0L), noBlocksInTiers);
    mMaster.workerRegister(worker2, Arrays.asList(100L), Arrays.asList(0L), noBlocksInTiers);
    mMaster.commitBlock(worker1, 1L, 1, 1L, 1L);
    mMaster.commitBlock(worker1, 2L, 1, 2L, 1L);
    mMaster.commitBlock(worker1, 3L, 1, 3L, 1L);
    mMaster.commitBlock(worker2, 1L, 1, 1L, 1L);
    mMaster.commitBlock(worker2, 2L, 1, 2L, 1L);
    mMaster.commitBlock(worker2, 3L, 1, 3L, 1L);
    mMaster.removeBlocks(workerBlocks);
  }

  @Test
  public void workerHeartbeatTest() throws Exception {
    mMaster.start(true);
    long workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    MasterWorkerInfo workerInfo = mPrivateAccess.getWorkerById(workerId);
    final List<Long> USED_BYTES_ON_TIERS = ImmutableList.of(125L);
    final List<Long> INITIAL_BLOCKS = ImmutableList.of(1L, 2L);
    final int MEM_TIER = StorageLevelAlias.MEM.getValue();
    addWorker(mMaster, workerId, ImmutableList.of(500L), USED_BYTES_ON_TIERS);
    for (Long block : INITIAL_BLOCKS) {
      mMaster.commitBlock(workerId, USED_BYTES_ON_TIERS.get(0), MEM_TIER, block, 100L);
    }

    // test heartbeat removing a block
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    final long REMOVED_BLOCK = INITIAL_BLOCKS.get(0);
    Command heartBeat1 =
        mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS, ImmutableList.of(REMOVED_BLOCK),
            ImmutableMap.<Long, List<Long>>of());
    Set<Long> expectedBlocks =
        Sets.difference(ImmutableSet.copyOf(INITIAL_BLOCKS), ImmutableSet.of(REMOVED_BLOCK));
    // block is removed from worker info
    Assert.assertEquals(expectedBlocks, workerInfo.getBlocks());
    // worker is removed from block info
    Assert.assertEquals(ImmutableSet.of(), mPrivateAccess.getMasterBlockInfo(REMOVED_BLOCK)
        .getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat1);

    // test heartbeat adding back the block
    List<Long> readdedBlocks = ImmutableList.of(REMOVED_BLOCK);
    Command heartBeat2 =
        mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS, ImmutableList.<Long>of(),
            ImmutableMap.of((long) MEM_TIER, readdedBlocks));
    // block is restored to worker info
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    // worker is restored to block info
    Assert.assertEquals(ImmutableSet.of(workerId), mPrivateAccess.getMasterBlockInfo(REMOVED_BLOCK)
        .getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);

    // test heartbeat where the master tells the worker to remove a block
    final long BLOCK_TO_FREE = INITIAL_BLOCKS.get(1);
    workerInfo.updateToRemovedBlock(true, BLOCK_TO_FREE);
    Command heartBeat3 =
        mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS, ImmutableList.<Long>of(),
            ImmutableMap.<Long, List<Long>>of());
    Assert.assertEquals(new Command(CommandType.Free, ImmutableList.<Long>of(BLOCK_TO_FREE)),
        heartBeat3);
  }

  @Test
  public void heartbeatStatusTest() throws Exception {
    mMaster.start(true);
    long workerId = mMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    MasterWorkerInfo workerInfo = mPrivateAccess.getWorkerById(workerId);
    final List<Long> INITIAL_USED_BYTES_ON_TIERS = ImmutableList.of(25L, 50L, 125L);
    addWorker(mMaster, workerId, ImmutableList.of(50L, 100L, 500L), INITIAL_USED_BYTES_ON_TIERS);

    long lastUpdatedTime1 = workerInfo.getLastUpdatedTimeMs();
    Thread.sleep(1); // sleep for 1ms so that lastUpdatedTimeMs is guaranteed to change
    final List<Long> NEW_USED_BYTES_ON_TIERS = ImmutableList.of(50L, 100L, 500L);
    // test simple heartbeat letting the master know that more bytes are being used
    Command heartBeat =
        mMaster.workerHeartbeat(workerId, NEW_USED_BYTES_ON_TIERS, ImmutableList.<Long>of(),
            ImmutableMap.<Long, List<Long>>of());
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

  private void addWorker(BlockMaster master, long workerId, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers) {
    master.workerRegister(workerId, totalBytesOnTiers, usedBytesOnTiers,
        Maps.<Long, List<Long>>newHashMap());
  }
}
