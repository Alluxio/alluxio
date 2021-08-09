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

import com.beust.jcommander.Parameter;

/**
 * Parameters for generating different stress on the BlockMaster.
 */
public class BlockMasterBenchParameters extends RpcBenchParameters {
  @Parameter(names = {"--tiers"},
      description = "The number of blocks in each storage dir and tier. "
          + "Use semi-colon to separate tiers, use commas to separate dirs. "
          + "Example: \"100,200,300;1000,1500;2000\"")
  public String mTiers;
}
