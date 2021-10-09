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

package alluxio.stress.fuse;

import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * This class holds all the Fuse IO related parameters. All fields are public for easier json
 * ser/de without all the getters and setters.
 */
public final class FuseIOParameters extends Parameters {
  @Parameter(names = {"--operation"},
      description = "The operation to perform. Options are [LocalRead, RemoteRead, ClusterRead, "
          + "Write, ListFile], where \"Write\" and \"ListFile\" are for testing read performance, "
          + "not individual tests.",
      converter = FuseIOOperationConverter.class,
      required = true)
  public FuseIOOperation mOperation;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public int mThreads = 1;

  @Parameter(names = {"--local-path"},
      description = "The local filesystem directory or Fuse mount point to perform operations in")
  public String mLocalPath = "/mnt/alluxio-fuse/fuse-io-bench";

  @Parameter(names = {"--file-size"},
      description = "The files size for IO operations. (100k, 1m, 1g, etc.)")
  public String mFileSize = "100k";

  @Parameter(names = {"--buffer-size"},
      description = "The buffer size for IO operations. (1k, 16k, etc.)")
  public String mBufferSize = "64k";

  @Parameter(names = {"--num-files-per-dir"},
      description = "The number of files per directory")
  public int mNumFilesPerDir = 1000;

  @Parameter(names = {"--num-dirs"},
      description = "The number of directories that the files will be evenly distributed into."
          + "It must be at least the number of threads and preferably a multiple of it.")
  public int mNumDirs = 1;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "15s";

  /**
   * Converts from String to FuseIOOperation instance.
   *
   * @return FuseIOOperation of this bench
   */
  public static class FuseIOOperationConverter implements IStringConverter<FuseIOOperation> {
    @Override
    public FuseIOOperation convert(String value) {
      return FuseIOOperation.fromString(value);
    }
  }
}
