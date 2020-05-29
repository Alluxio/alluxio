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

package alluxio.stress.worker;

import alluxio.stress.Parameters;

import com.beust.jcommander.Parameter;

/**
 * Parameters used in the UFS I/O throughput test.
 * */
public class WorkerBenchParameters extends Parameters {
  @Parameter(names = {"--threads"}, description = "the number of threads to use")
  public int mThreads = 16;

  @Parameter(names = {"--io-size"},
          description = "size of data to write or read in total, in MB")
  public int mDataSize = 4096;

  @Parameter(names = {"--path"},
          description = "the Alluxio directory to write temporary data in",
          required = true)
  public String mPath;

  @Parameter(names = {"--workers"},
          description = "the number of workers to use")
  public int mWorkerNum = 1;
}
