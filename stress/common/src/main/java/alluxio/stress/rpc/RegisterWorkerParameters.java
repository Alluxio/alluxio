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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters used in the UFS I/O throughput test.
 * */
public class RegisterWorkerParameters extends RpcBenchParameters {

  @Parameter(names = {"--fake-same-worker"},
          description = "For each registerWorker RPC, should this be same worker or different worker.")
  public boolean mSameWorker = false;

  @Parameter(names = {"--tiers"})
  /**
   * Examples: "100,200,300;1000,1500;2000"
   * Use semi-colon to separate tiers, use commas to separate dirs
   * */
  public String mTiers;
}
