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

import java.util.Collections;
import java.util.List;

/**
 * This class holds all the Fuse IO related parameters. All fields are public for easier json
 * ser/de without all the getters and setters.
 */
public final class FuseIOParameters extends Parameters {
  /** This must match the member name. */
  public static final String FIELD_READ_RANDOM = "mReadRandom";

  @Parameter(names = {"--operation"},
      description = "the operation to perform. Options are [read]",
      converter = FuseIOOperationConverter.class,
      required = true)
  public FuseIOOperation mOperation;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public List<Integer> mThreads = Collections.singletonList(1);

  @Parameter(names = {"--localPath"},
      description = "The local filesystem directory to perform operations in")
  public String mLocalPath = "/mnt/alluxio-fuse";

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "15s";

  @Parameter(names = {"--read-random"},
      description = "If true, threads read the file from random offsets. For streaming "
          + "operations, seek() is called to read random offsets. If false, the file is read "
          + "sequentially.")
  @Parameters.BooleanDescription(trueDescription = "Random", falseDescription = "Sequential")
  public boolean mReadRandom = false;

  /**
   * Converts from String to FuseIOOperation instance.
   * @return FuseIOOperation of this bench
   */
  public static class FuseIOOperationConverter implements IStringConverter<FuseIOOperation> {
    @Override
    public FuseIOOperation convert(String value) {
      return FuseIOOperation.fromString(value);
    }
  }
}
