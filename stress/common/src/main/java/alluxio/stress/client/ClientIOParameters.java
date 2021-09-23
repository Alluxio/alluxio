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
import alluxio.stress.common.FileSystemParameters;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class ClientIOParameters extends FileSystemParameters {
  /** This must match the member name. */
  public static final String FIELD_READ_RANDOM = "mReadRandom";

  @Parameter(names = {"--operation"},
      description = "the operation to perform. Options are [ReadArray, ReadByteBuffer, ReadFully,"
          + " PosRead, PosReadFully]",
      converter = ClientIOOperationConverter.class,
      required = true)
  public ClientIOOperation mOperation;

  @Parameter(names = {"--clients"}, description = "the number of fs client instances to use")
  public int mClients = 1;

  @Parameter(names = {"--threads"},
      description = "the comma-separated list of thread counts to test. The throughput for each "
          + "thread count is benchmarked and measured separately.")
  public List<Integer> mThreads = Collections.singletonList(1);

  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  @Parameters.PathDescription(aliasFieldName = "mBaseAlias")
  public String mBasePath = "alluxio://localhost:19998/stress-client-io-base";

  @Parameter(names = {"--base-alias"}, description = "The alias for the base path, unused if empty")
  @Parameters.KeylessDescription
  public String mBaseAlias = "";

  @Parameter(names = {"--tag"}, description = "optional human-readable string to identify this run")
  @Parameters.KeylessDescription
  public String mTag = "";

  @Parameter(names = {"--file-size"},
      description = "The files size for IO operations. (1g, 4g, etc.)")
  public String mFileSize = "1g";

  @Parameter(names = {"--buffer-size"},
      description = "The buffer size for IO operations. (1k, 16k, etc.)")
  public String mBufferSize = "64k";

  @Parameter(names = {"--block-size"},
      description = "The block size of files. (16k, 64m, etc.)")
  public String mBlockSize = "64m";

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "30s";

  @Parameter(names = {"--read-same-file"},
      description = "If true, all threads read from the same file. Otherwise, each thread reads "
          + "from its own file.")
  @Parameters.BooleanDescription(trueDescription = "SameFile", falseDescription = "OwnFile")
  public boolean mReadSameFile = false;

  @Parameter(names = {"--read-random"},
      description = "If true, threads read the file from random offsets. For streaming "
          + "operations, seek() is called to read random offsets. If false, the file is read "
          + "sequentially.")
  @Parameters.BooleanDescription(trueDescription = "Random", falseDescription = "Sequential")
  public boolean mReadRandom = false;

  @Parameter(names = {"--write-num-workers"},
      description = "The number of workers to distribute the files to. The blocks of a written "
          + "file will be round-robin across these number of workers.")
  public int mWriteNumWorkers = 1;

  @DynamicParameter(names = "--conf",
      description = "Any HDFS client configuration key=value. Can repeat to provide multiple "
          + "configuration values.")

  public Map<String, String> mConf = new HashMap<>();

  /**
   * @return ClientIOOperation of this bench
   * Converts from String to ClientIOOperation instance.
   */
  public static class ClientIOOperationConverter implements IStringConverter<ClientIOOperation> {
    @Override
    public ClientIOOperation convert(String value) {
      return ClientIOOperation.fromString(value);
    }
  }
}
