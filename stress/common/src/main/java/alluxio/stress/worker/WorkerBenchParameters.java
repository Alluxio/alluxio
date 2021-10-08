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
import alluxio.stress.common.FileSystemParameters;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class WorkerBenchParameters extends FileSystemParameters {
  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  @Parameters.PathDescription(aliasFieldName = "mBaseAlias")
  public String mBasePath = "alluxio://localhost:19998/stress-worker-base";

  @Parameter(names = {"--base-alias"}, description = "The alias for the base path, unused if empty")
  @Parameters.KeylessDescription
  public String mBaseAlias = "";

  @Parameter(names = {"--tag"}, description = "A string to identify this run")
  @Parameters.KeylessDescription
  public String mTag = "";

  @Parameter(names = {"--clients"}, description = "the number of fs clients to use")
  public int mClients = 1;

  @Parameter(names = {"--threads"}, description = "the number of threads to use")
  public int mThreads = 256;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "30s";

  @Parameter(names = {"--file-size"},
      description = "The files size for IO operations. (1g, 4g, etc.)")
  public String mFileSize = "128m";

  @Parameter(names = {"--buffer-size"},
      description = "The buffer size for IO operations. (1k, 16k, etc.)")
  public String mBufferSize = "4k";

  @Parameter(names = {"--block-size"},
      description = "The size of the file block. (16k, 64m, etc.)")
  public String mBlockSize = "32m";

  @Parameter(names = {"--free"},
      description = "If true, free the data from Alluxio before reading. Only applies to Alluxio "
          + "paths")
  public boolean mFree = false;

  @DynamicParameter(names = "--conf", description = "HDFS client configuration. Can be repeated.")
  public Map<String, String> mConf = new HashMap<>();
}
