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
      description = "Path of the base directory where each subdirectory contains source files "
          + "to compact")
  public String mSourceBase = "/compaction-base/source";

  @Parameter(names = {"--source-dirs"},
      description = "Number of directories containing source files to compact. "
      + "In cluster mode, each job worker will create this many directories.")
  public int mNumSourceDirs = 100;

  @Parameter(names = {"--source-files"},
      description = "Number of files to compact in each subdirectories.")
  public int mNumSourceFiles = 100;

  @Parameter(names = {"--source-file-size"},
      description = "Size of the source files.")
  public String mSourceFileSize = "8kb";

  @Parameter(names = {"--output-base"},
      description = "Path of the base directory where compacted output will be stored.")
  public String mOutputBase = "/compaction-base/output";

  @Parameter(names = {"--staging-base"},
      description = "Path of the staging directory where intermediate files are created.")
  public String mStagingBase = "/compaction-base/.staging";

  @Parameter(names = {"--output-in-place"},
      description = "Whether to output each compacted file in the same directory of its "
          + "source files. If this is set to true, the --output-base option is ignored.")
  public boolean mOutputInPlace = false;

  @Parameter(names = {"--threads"},
      description = "Number of active parallel compaction threads at one time.")
  public int mThreads = 1;

  @Parameter(names = {"--delay"},
      description = "Time to wait after the last job is done and before the next job "
          + "is started on one thread.")
  public String mDelayMs = "0s";

  @Parameter(names = {"--compact-ratio"},
      description = "Ratio of source files to output compacted files. E.g. 10 means every 10 "
          + "source files get compacted into 1 big file.")
  public int mCompactRatio = 10;

  @Parameter(names = {"--read-buf-size"},
      description = "Size of the buffer for reading from the source files in one read call.")
  public String mBufSize = "4kb";

  @Parameter(names = {"--skip-prepare"},
      description = "Skip re-creating test directories and files.")
  public boolean mSkipPrepare = false;

  @Parameter(names = {"--preserve-source"},
      description = "Set this flag to preserve the source files after the output is written.")
  @BooleanDescription(trueDescription = "Preserve", falseDescription = "Delete")
  public boolean mPreserveSource = false;

  @Parameter(names = {"--delete-by-dir"},
      description = "Set this flag to delete the source dir recursively, instead of deleting "
          + "files individually. This is ignored if --preserve-source is set.")
  public boolean mDeleteByDir = false;
}
