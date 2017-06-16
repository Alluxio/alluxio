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

package alluxio.client.file.policy;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link SpecificHostPolicy}.
 */
public final class SpecificHostPolicyTest {
  private static final int PORT = 1;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  /**
   * Tests that the correct worker is returned when using the policy.
   */
  @Test
  public void policy() throws Exception {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker2");
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    Assert.assertEquals("worker2",
        policy.getWorkerForNextBlock(workerInfoList, Constants.MB).getHost());
  }

  /**
   * Tests that no worker is chosen when the worker specified in the policy is not part of the
   * worker list.
   */
  @Test
  public void noMatchingHost() throws Exception {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker3");
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    mExpectedException.expect(UnavailableException.class);
    mExpectedException.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER
        .getMessage(Constants.MB));
    policy.getWorkerForNextBlock(workerInfoList, Constants.MB);
  }
}
