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

package alluxio.stress.rpc;

import alluxio.stress.Parameters;

import com.beust.jcommander.Parameter;

/**
 * Parameters for the RPC benchmark test.
 */
public class RpcBenchParameters extends Parameters {
  @Parameter(names = {"--concurrency"},
      description = "simulate this many clients/workers on one machine")
  public int mConcurrency = 2;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "5s";
}
