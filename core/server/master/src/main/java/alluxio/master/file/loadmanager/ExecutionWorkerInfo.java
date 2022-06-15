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

package alluxio.master.file.loadmanager;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Class for assigned worker info.
 */
public class ExecutionWorkerInfo {
  /**
   * Execute a task.
   * @param client blockWorkerClient
   * @param loadRequest loadRequest
   * @return LoadResponse
   */
  public static ListenableFuture<LoadResponse> execute(
          BlockWorkerClient client, LoadRequest loadRequest) {
    return client.load(loadRequest);
  }
}
