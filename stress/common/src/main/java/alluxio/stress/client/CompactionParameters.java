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

import com.beust.jcommander.Parameter;

public class CompactionParameters extends Parameters {
  @Parameter(names = {"--source-base"},
      description = "URI of the base directory where each subdirectory contains source files "
          + "to compact")
  public String mSourceBase = "alluxio://localhost:19998/compaction-source";

  @Parameter(names = {"--output-base"},
      description = "URI of the base directory where compacted output will be written to.")
  public String mOutputBase = "alluxio://localhost:19998/compaction-output";

  @Parameter(names = {"--output-in-place"},
      description = "Whether to output each compacted file in the same directory of its "
          + "source files. If this is set to true, the --output-base option is ignored.")
  public boolean mOutputInPlace = false;

  @Parameter(names = {"--threads"},
      description = "Number of active parallel compaction threads at one time.")
  public int mThreads = 1;

  @Parameter(names = {"--delay"},
      description = "Milliseconds to wait after the last job is done and before the next job "
          + "is started on one thread.")
  public int mDelayMs = 0;

  @Parameter(names = {"--compact-ratio"},
      description = "Ratio of source files to output compacted files. E.g. 10 means every 10 "
          + "source files get compacted into 1 big file.")
  public int mCompactRatio = 10;

  @Parameter(names = {"--preserve-source"},
      description = "Set this flag to preserve the source files after the output is written.")
  @BooleanDescription(trueDescription = "Preserve", falseDescription = "Delete")
  public boolean mPreserveSource = false;
}
