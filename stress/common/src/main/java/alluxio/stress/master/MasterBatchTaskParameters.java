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

package alluxio.stress.master;

import alluxio.stress.common.BatchTaskParameters;

import com.beust.jcommander.Parameter;

/**
 * This holds all the parameters for Master Batch Task.
 */
public class MasterBatchTaskParameters extends BatchTaskParameters {
  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  public String mBasePath = "alluxio:///stress-master-base";

  @Parameter(names = {"--num-files"}, description = "the number of files to operate on")
  public int mNumFiles = 10000;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public int mThreads = 256;

  @Parameter(names = {"--create-file-size"},
      description = "The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)")
  public String mFileSize = "0";
}
