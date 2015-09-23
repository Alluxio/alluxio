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
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;

public class LineageEntry implements JournalEntry {
  private final long mId;
  private final List<Long> mInputFiles;
  private final List<LineageFileEntry> mOutputFiles;
  private final JobEntry mJob;
  private final long mCreationTimeMs;

  public LineageEntry(long id, List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job,
      long creationTimeMs) {
    mId = id;
    mInputFiles = Lists.newArrayList();
    for (TachyonFile file : inputFiles) {
      mInputFiles.add(file.getFileId());
    }
    mOutputFiles = Lists.newArrayList();
    for (LineageFile file : outputFiles) {
      mOutputFiles
          .add(new LineageFileEntry(file.getFileId(), file.getState(), file.getUnderFilePath()));
    }
    // TODO(yupeng) support other job types
    Preconditions.checkState(job instanceof CommandLineJob);
    CommandLineJob commandLineJob = (CommandLineJob) job;
    mJob =
        new JobEntry(commandLineJob.getJobConf().getOutputFilePath(), commandLineJob.getCommand());
    mCreationTimeMs = creationTimeMs;
  }

  public Lineage toLineage() {
    List<TachyonFile> inputFiles = Lists.newArrayList();
    for (long file : mInputFiles) {
      inputFiles.add(new TachyonFile(file));
    }

    List<LineageFile> outputFiles = Lists.newArrayList();
    for (LineageFileEntry lineageFileEntry : mOutputFiles) {
      outputFiles.add(lineageFileEntry.toLineageFile());
    }

    Job job = mJob.toJob();

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
    parameters.put("outputFiles", mOutputFiles);
    parameters.put("job", mJob);
    parameters.put("creationTimeMs", mCreationTimeMs);
    return parameters;
  }

}
