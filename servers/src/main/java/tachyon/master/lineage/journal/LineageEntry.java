/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage.journal;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.client.file.TachyonFile;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageFileState;

public class LineageEntry implements JournalEntry {
  private final long mId;
  private final List<Long> mInputFiles;
  private final List<Long> mOutputFileIds;
  // TODO(yupeng) allow journal entry to have nested class
  private final List<LineageFileState> mOutputFileStates;
  private final List<String> mOutputFileUnderFsPaths;
  private final String mJobCommand;
  private final String mJobOutputPath;
  private final long mCreationTimeMs;

  public LineageEntry(long id, List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job,
      long creationTimeMs) {
    mId = id;
    mInputFiles = Lists.newArrayList();
    for (TachyonFile file : inputFiles) {
      mInputFiles.add(file.getFileId());
    }
    mOutputFileIds = Lists.newArrayList();
    mOutputFileStates = Lists.newArrayList();
    mOutputFileUnderFsPaths = Lists.newArrayList();
    for (LineageFile file : outputFiles) {
      mOutputFileIds.add(file.getFileId());
      mOutputFileStates.add(file.getState());
      mOutputFileUnderFsPaths.add(file.getUnderFilePath());
    }
    // TODO(yupeng) support other job types
    Preconditions.checkState(job instanceof CommandLineJob);
    CommandLineJob commandLineJob = (CommandLineJob) job;
    mJobCommand = commandLineJob.getCommand();
    mJobOutputPath = commandLineJob.getJobConf().getOutputFilePath();
    mCreationTimeMs = creationTimeMs;
  }

  public Lineage toLineage() {
    List<TachyonFile> inputFiles = Lists.newArrayList();
    for (long file : mInputFiles) {
      inputFiles.add(new TachyonFile(file));
    }

    List<LineageFile> outputFiles = Lists.newArrayList();
    for (int i = 0; i < mOutputFileIds.size(); i ++) {
      outputFiles.add(new LineageFile(mOutputFileIds.get(i), mOutputFileStates.get(i),
          mOutputFileUnderFsPaths.get(i)));
    }

    Job job = new CommandLineJob(mJobCommand, new JobConf(mJobOutputPath));

    return new Lineage(mId, inputFiles, outputFiles, job, mCreationTimeMs);
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.LINEAGE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(5);
    parameters.put("id", mId);
    parameters.put("inputFiles", mInputFiles);
    parameters.put("outputFileIds", mOutputFileIds);
    parameters.put("outputFileStates", mOutputFileStates);
    parameters.put("outputFileUnderFsPaths", mOutputFileStates);
    parameters.put("jobCommand", mJobCommand);
    parameters.put("jobOutputPath", mJobOutputPath);
    parameters.put("creationTimeMs", mCreationTimeMs);
    return parameters;
  }

}
