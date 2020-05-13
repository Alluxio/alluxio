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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class ClientIOParameters extends Parameters {
  @Parameter(names = {"--operation"},
      description = "the operation to perform. Options are [ReadArray, ReadByteBuffer, ReadFully,"
          + " PosRead, PosReadFully]",
      required = true)
  public Operation mOperation;

  @Parameter(names = {"--clients"}, description = "the number of fs clients to use")
  public int mClients = 1;

  @Parameter(names = {"--threads"},
      description = "the comma-separated list of thread counts to test. The throughput for each "
          + "thread tested is benchmarked.")
  public List<Integer> mThreads = Collections.singletonList(1);

  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  public String mBasePath = "alluxio://localhost:19998/stress-client-io-base";

  @Parameter(names = {"--file-size"},
      description = "The files size for IO operations. (1g, 4g, etc.)")
  public String mFileSize = "1g";

  @Parameter(names = {"--buffer-size"},
      description = "The buffer size for IO operations. (1k, 16k, etc.)")
  public String mBufferSize = "64k";

  @Parameter(names = {"--block-size"},
      description = "The size of the file block. (16k, 64m, etc.)")
  public String mBlockSize = "64m";

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "30s";

  @Parameter(names = {"--read-same-file"},
      description = "If true, read the same file.")
  public boolean mReadSameFile = false;

  @Parameter(names = {"--read-random"},
      description = "If true, read the file from random offsets. For stream operations, seek() is"
          + " called to read random offsets.")
  public boolean mReadRandom = false;

  @Parameter(names = {"--write-num-workers"},
      description = "The number of workers to distribute the files to. The blocks of a written "
          + "file will be round-robin across these number of workers.")
  public int mWriteNumWorkers = 1;

  @DynamicParameter(names = "--conf", description = "HDFS client configuration. Can be repeated.")
  public Map<String, String> mConf = new HashMap<>();

  @Override
  public String prettyPrintDescriptionField(String fieldName, Object value) {
    if ("mReadRandom".equals(fieldName)) {
      if ((Boolean) value) {
        return "Random";
      }
      return "Sequential";
    }
    if ("mReadSameFile".equals(fieldName)) {
      if ((Boolean) value) {
        return "SameFile";
      }
      return "OwnFile";
    }
    return super.prettyPrintDescriptionField(fieldName, value);
  }
}
