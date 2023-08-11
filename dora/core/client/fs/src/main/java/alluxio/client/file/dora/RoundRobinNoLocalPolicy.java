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

//package alluxio.client.file.dora;
//
//import alluxio.client.block.BlockWorkerInfo;
//
//import java.util.List;
//
///**
// TODO(jiacheng): this relies on the singleton worker service finder
// * A policy where work is distributed evenly across remote workers using round-robin algorithm.
// *
// * This policy can ONLY be used in tests! This is because this policy breaks the property of being
// * deterministic in the {@link WorkerLocationPolicy}. By round-robin, a path /a is allocated to
// * worker A on this client but may be allocated to worker B on another client. So cache on worker
// * A will not be found by the 2nd client!
// */
//public class RoundRobinNoLocalPolicy implements WorkerLocationPolicy  {
//  // In order to achieve balance, this policy instance must be a singleton.
//  private static final RoundRobinNoLocalPolicy INSTANCE = new RoundRobinNoLocalPolicy();
//
//  private RoundRobinNoLocalPolicy() {
//
//  }
//
//  @Override
//  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
//                                                   String fileId, int count) {
//    // After we extract a centralized worker-list maintainer, rely on that
//
//    // If there's no change, allocate to the next one in the iterator
//
//    // Actually, if the worker list has changed, we can just abort and fail the test
//    return null;
//  }
//}
