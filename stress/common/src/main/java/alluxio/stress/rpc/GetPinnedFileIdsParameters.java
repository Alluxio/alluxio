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
 * Parameters for the stress test on GetPinnedFileIds RPC.
 */
public class GetPinnedFileIdsParameters extends RpcBenchParameters {
  @Parameter(names = {"--num-files"},
      description = "The number of pinned files to generate in the test.")
  public int mNumFiles = 100;

  @Parameter(names = {"--base-dir"},
      description = "The test directory will be created and hold all the test files. "
          + "At the end of the test, this directory will be removed.")
  public String mBasePath = "/get-pin-list-bench-base";
}
