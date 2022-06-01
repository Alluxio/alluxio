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

package alluxio.stress.jobservice;

import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class JobServiceBenchParameters extends Parameters {
  @Parameter(names = {"--operation"},
      description = "the operation to perform.",
      converter = OperationConverter.class,
      required = true)
  public JobServiceBenchOperation mOperation;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public int mThreads = 256;

  @Parameter(names = {"--files-per-dir"}, description = "the number of files in each directory.")
  public int mNumFilesPerDir = 1000;

  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  public String mBasePath = "alluxio:///stress-job-service-base";

  @Parameter(names = {"--file-size"},
      description = "The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)")
  public String mFileSize = "1k";

  @Parameter(names = {"--target-throughput"},
      description = "the target throughput to issue operations. (ops / s)")
  public int mTargetThroughput = 1000;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "30s";

  @Parameter(names = {"--batch-size"}, description = "The batch size of operations")
  public int mBatchSize = 1;

  @Override
  public Enum<?> operation() {
    return mOperation;
  }

  /**
   * Converts from String to Operation instance.
   */
  public static class OperationConverter implements IStringConverter<JobServiceBenchOperation> {
    @Override
    public JobServiceBenchOperation convert(String value) {
      return JobServiceBenchOperation.fromString(value);
    }
  }
}
