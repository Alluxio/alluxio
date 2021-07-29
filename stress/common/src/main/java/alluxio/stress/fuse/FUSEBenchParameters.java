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
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class FUSEBenchParameters extends Parameters {
  @Parameter(names = {"--operation"},
          description = "the operation to perform. Options are [read]",
          converter = FUSEBenchOperationConverter.class,
          required = true)
  public FUSEBenchOperation mOperation;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public int mThreads = 1;

  @Parameter(names = {"--base"},
          description = "The base directory path URI to perform operations in")

  @Parameters.PathDescription(aliasFieldName = "mBaseAlias")
  public String mBasePath = "/mnt/alluxio-fuse/stress-fuse-";

  @Parameter(names = {"--duration"},
          description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
          description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
                  + "10000ms, etc.)")
  public String mWarmup = "30s";

  /**
   * @return FUSEBenchOperation of this bench
   * Converts from String to FUSEBenchOperation instance.
   */
  public static class FUSEBenchOperationConverter implements IStringConverter<FUSEBenchOperation> {
    @Override
    public FUSEBenchOperation convert(String value) { return FUSEBenchOperation.fromString(value); }
  }
}
