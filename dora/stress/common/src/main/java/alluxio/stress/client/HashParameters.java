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

package alluxio.stress.client;

import alluxio.stress.Parameters;

import com.beust.jcommander.Parameter;

/**
 * Hash test parameters that users can pass in,
 * through which the user sets the hash strategy,
 * number of virtual nodes, number of node replicas,
 * lookup table size, etc.
 */
public class HashParameters extends Parameters {

  @Parameter(names = {"--hash-policy"},
      description = "Use the hash policy to test. "
      + "If you want to test multiple hash policies, please separate them with \",\", "
      + "such as \"CONSISTENT,MAGLEV\". "
      + "There are currently five supported policies: "
      + "CONSISTENT, JUMP, KETAMA, MAGLEV, MULTI_PROBE")
  public String mHashPolicy = "CONSISTENT,JUMP,KETAMA,MAGLEV,MULTI_PROBE";

  @Parameter(names = {"--virtual-node-num"}, description = "the number of virtual nodes")
  public Integer mVirtualNodeNum = 1000;

  @Parameter(names = {"--worker-num"}, description = "the number of workers")
  public Integer mWorkerNum = 10;

  @Parameter(names = {"--node-replicas"}, description = "the number of ketama hashing replicas")
  public Integer mNodeReplicas = 1000;

  @Parameter(names = {"--lookup-size"},
      description = "the size of the lookup table in the maglev hashing algorithm")
  public Integer mLookupSize = 65537;

  @Parameter(names = {"--probe-num"},
      description = "the number of probes in the multi-probe hashing algorithm")
  public Integer mProbeNum = 21;

  @Parameter(names = {"--report-path"}, description = "the path that the report will generate on")
  public String mReportPath = ".";

  @Parameter(names = {"--file-num"}, description = "the num of files that will be allocated")
  public Integer mFileNum = 1000000;
}
