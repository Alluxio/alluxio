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

package alluxio.client.block.policy.options;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.test.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Tests for the {@link GetWorkerOptions} class.
 */
public final class GetWorkerOptionsTest {
  private static final int PORT = 1;

  private final List<BlockWorkerInfo> mWorkerInfos = new ArrayList<>();

  /**
   * Tests for defaults {@link GetWorkerOptions}.
   */
  @Test
  public void defaults() throws IOException {
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockInfo(new BlockInfo());
    assertEquals(null, options.getBlockWorkerInfos());
    assertEquals(0, options.getBlockInfo().getBlockId());
    assertEquals(0, options.getBlockInfo().getLength());
  }

  /**
   * Tests for setBlockWorkerInfo.
   */
  @Test
  public void setBlockWorkerInfoTest() {
    mWorkerInfos.clear();
    mWorkerInfos.add(new BlockWorkerInfo(
            new WorkerNetAddress().setHost("worker1").setRpcPort(PORT).setDataPort(PORT)
                    .setWebPort(PORT), Constants.GB, 0));
    mWorkerInfos.add(new BlockWorkerInfo(
            new WorkerNetAddress().setHost("worker2").setRpcPort(PORT).setDataPort(PORT)
                    .setWebPort(PORT), 2 * (long) Constants.GB, 0));
    GetWorkerOptions options = GetWorkerOptions.defaults();
    options.setBlockWorkerInfos(mWorkerInfos);
    assertEquals(mWorkerInfos, options.getBlockWorkerInfos());
  }

  /**
   * Tests for setBlockId and setBlockSize.
   */
  @Test
  public void fields() {
    Random rand = new Random();
    long blockId = rand.nextLong();
    long blockSize = rand.nextLong();
    GetWorkerOptions options = GetWorkerOptions.defaults();
    BlockInfo info = new BlockInfo().setBlockId(blockId).setLength(blockSize);
    options.setBlockInfo(info);
    assertEquals(info, options.getBlockInfo());
  }

  @Test
  public void equalTest() throws Exception {
    CommonUtils.testEquals(GetWorkerOptions.class);
  }
}
