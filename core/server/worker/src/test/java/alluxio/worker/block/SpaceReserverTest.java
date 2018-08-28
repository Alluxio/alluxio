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

package alluxio.worker.block;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link SpaceReserver}.
 */
public class SpaceReserverTest {
  private ExecutorService mExecutorService;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public ManuallyScheduleHeartbeat mSchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.WORKER_SPACE_RESERVER);

  @Before
  public void before() {
    mExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.build("space-reserver-test", true));
  }

  @After
  public void after() {
    mExecutorService.shutdownNow();
  }

  @Test
  public void reserveCorrectAmountsOfSpace() throws Exception {
    BlockWorker blockWorker = mock(BlockWorker.class);
    BlockStoreMeta storeMeta = mock(BlockStoreMeta.class);
    when(blockWorker.getStoreMeta()).thenReturn(storeMeta);
    Map<String, Long> capacityBytesOnTiers = ImmutableMap.of("MEM", 400L, "HDD", 1000L);
    when(storeMeta.getCapacityBytesOnTiers()).thenReturn(capacityBytesOnTiers);

    String tmpFolderPath = mTempFolder.newFolder().getAbsolutePath();

    // Create two tiers named "MEM" and "HDD" with aliases 0 and 1.
    TieredBlockStoreTestUtils.setupConfWithMultiTier(tmpFolderPath,
        new int[]{0, 1}, new String[] {"MEM", "HDD"},
        new String[][]{new String[]{"/a"}, new String[]{"/b"}},
        new long[][]{new long[]{0}, new long[]{0}}, "/");

    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO, "0.2",
        PropertyKey.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO, "0.3")).toResource()) {
      SpaceReserver spaceReserver = new SpaceReserver(blockWorker);

      mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER,
          spaceReserver, 0));

      // Run the space reserver once.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_SPACE_RESERVER);

      // 400 * 0.2 = 80
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 80L, "MEM");
      // 400 * 0.2 + 1000 * 0.3 = 380
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 380L, "HDD");
    }
  }

  @Test
  public void testLowWatermark() throws Exception {
    BlockWorker blockWorker = mock(BlockWorker.class);
    BlockStoreMeta storeMeta = mock(BlockStoreMeta.class);
    when(blockWorker.getStoreMeta()).thenReturn(storeMeta);
    Map<String, Long> capacityBytesOnTiers = ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD",
        1000L);
    Map<String, Long> usedCapacityBytesOnTiers = ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD",
        1000L);
    when(storeMeta.getCapacityBytesOnTiers()).thenReturn(capacityBytesOnTiers);
    when(storeMeta.getUsedBytesOnTiers()).thenReturn(usedCapacityBytesOnTiers);

    String tmpFolderPath = mTempFolder.newFolder().getAbsolutePath();

    // Create two tiers named "MEM", "SSD" and "HDD" with aliases 0, 1 and 2.
    TieredBlockStoreTestUtils.setupConfWithMultiTier(tmpFolderPath,
        new int[]{0, 1, 2}, new String[] {"MEM", "SSD", "HDD"},
        new String[][]{new String[]{"/a"}, new String[]{"/b"}, new String[]{"/c"}},
        new long[][]{new long[]{0}, new long[]{0}, new long[]{0}}, "/");
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO, "0.9");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO, "0.8");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO, "0.9");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO, "0.7");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO, "0.8");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO, "0.6");
      }
    }).toResource()) {
      SpaceReserver spaceReserver = new SpaceReserver(blockWorker);

      mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER,
          spaceReserver, 0));

      // Run the space reserver once.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_SPACE_RESERVER);

      // 1000 * 0.4 + 200 * 0.3 + 100 * 0.2 = 480
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 480L, "HDD");
      // 200 * 0.3 + 100 * 0.2 = 80
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 80L, "SSD");
      // 100 * 0.2 = 20
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 20L, "MEM");
    }
  }

  @Test
  public void testHighWatermark() throws Exception {
    BlockWorker blockWorker = mock(BlockWorker.class);
    BlockStoreMeta storeMeta = mock(BlockStoreMeta.class);
    when(blockWorker.getStoreMeta()).thenReturn(storeMeta);
    Map<String, Long> capacityBytesOnTiers = ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD",
        1000L);
    Map<String, Long> usedCapacityBytesOnTiers =
        ImmutableMap.of("MEM", 100L, "SSD", 100L, "HDD", 0L);
    when(storeMeta.getCapacityBytesOnTiers()).thenReturn(capacityBytesOnTiers);
    when(storeMeta.getUsedBytesOnTiers()).thenReturn(usedCapacityBytesOnTiers);

    String tmpFolderPath = mTempFolder.newFolder().getAbsolutePath();

    // Create two tiers named "MEM", "SSD" and "HDD" with aliases 0, 1 and 2.
    TieredBlockStoreTestUtils.setupConfWithMultiTier(tmpFolderPath,
        new int[]{0, 1, 2}, new String[]{"MEM", "SSD", "HDD"},
        new String[][]{new String[]{"/a"}, new String[]{"/b"}, new String[]{"/c"}},
        new long[][]{new long[]{0}, new long[]{0}, new long[]{0}}, "/");
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO, "0.9");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO, "0.8");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO, "0.9");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO, "0.7");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO, "0.8");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO, "0.6");
      }
    }).toResource()) {
      SpaceReserver spaceReserver = new SpaceReserver(blockWorker);

      mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER,
          spaceReserver, 0));

      // Run the space reserver once.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_SPACE_RESERVER);

      // 1000 * 0.4 + 200 * 0.3 + 100 * 0.2 = 480
      verify(blockWorker, never()).freeSpace(
          Matchers.eq(Sessions.MIGRATE_DATA_SESSION_ID), Matchers.anyLong(), Matchers.eq("HDD"));
      // 200 * 0.3 + 100 * 0.2 = 80
      verify(blockWorker, never()).freeSpace(
          Matchers.eq(Sessions.MIGRATE_DATA_SESSION_ID), Matchers.anyLong(), Matchers.eq("SSD"));
      // 100 * 0.2 = 20
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 20L, "MEM");
    }
  }

  @Test
  public void smallWatermarkValues() throws Exception {
    BlockWorker blockWorker = mock(BlockWorker.class);
    BlockStoreMeta storeMeta = mock(BlockStoreMeta.class);
    when(blockWorker.getStoreMeta()).thenReturn(storeMeta);
    Map<String, Long> capacityBytesOnTiers =
        ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD", 1000L);
    Map<String, Long> usedCapacityBytesOnTiers =
        ImmutableMap.of("MEM", 100L, "SSD", 100L, "HDD", 0L);
    when(storeMeta.getCapacityBytesOnTiers()).thenReturn(capacityBytesOnTiers);
    when(storeMeta.getUsedBytesOnTiers()).thenReturn(usedCapacityBytesOnTiers);

    String tmpFolderPath = mTempFolder.newFolder().getAbsolutePath();
    // Create two tiers named "MEM", "SSD" and "HDD" with aliases 0, 1 and 2.
    TieredBlockStoreTestUtils.setupConfWithMultiTier(tmpFolderPath,
        new int[]{0, 1, 2}, new String[]{"MEM", "SSD", "HDD"},
        new String[][]{new String[]{"/a"}, new String[]{"/b"}, new String[]{"/c"}},
        new long[][]{new long[]{0}, new long[]{0}, new long[]{0}}, "/");
    try (Closeable c = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO, "0.4");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO, "0.3");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO, "0.3");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_LOW_WATERMARK_RATIO, "0.2");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO, "0.2");
        put(PropertyKey.WORKER_TIERED_STORE_LEVEL2_LOW_WATERMARK_RATIO, "0.1");
      }
    }).toResource()) {
      SpaceReserver spaceReserver = new SpaceReserver(blockWorker);

      mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER,
          spaceReserver, 0));

      // Run the space reserver once.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_SPACE_RESERVER);

      // 1000 * 0.1 + 200 = 300
      verify(blockWorker, never()).freeSpace(
          Matchers.eq(Sessions.MIGRATE_DATA_SESSION_ID), Matchers.anyLong(), Matchers.eq("HDD"));
      // 200 * 0.8 + 100 * 0.7 = 230 -> 200
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 200L, "SSD");
      // 100 * 0.7 = 70
      verify(blockWorker).freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, 70L, "MEM");
    }
  }
}
