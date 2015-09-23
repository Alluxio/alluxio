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

package tachyon.master.lineage.meta;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.client.file.TachyonFile;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.thrift.LineageFileInfo;
import tachyon.thrift.LineageInfo;

/**
 * A lineage tracks the dependencies imposed by a job, including the input files the job depends on,
 * and the output files the job generates.
 */
public final class Lineage {
  private final long mId;
  private final List<TachyonFile> mInputFiles;
  private final List<LineageFile> mOutputFiles;
  private final Job mJob;
  private final long mCreationTimeMs;

  /**
   * Creates a new lineage.
   *
   * @param inputFiles the input files.
   * @param outputFiles the output files.
   * @param job the job
   */
  public Lineage(List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job) {
    this(LineageIdGenerator.generateId(), inputFiles, outputFiles, job, System.currentTimeMillis());
  }

  /**
   * A method for lineage only. TODO(yupeng): hide this method
   *
   * @param inputFiles the input files.
   * @param inputFiles the input files.
   * @param outputFiles the output files.
   * @param job the job
   * @param creationTimeMs the creation time
   */
  public Lineage(long id, List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job,
      long creationTimeMs) {
    mInputFiles = Preconditions.checkNotNull(inputFiles);
    mOutputFiles = Preconditions.checkNotNull(outputFiles);
    mJob = Preconditions.checkNotNull(job);
    mId = id;
    mCreationTimeMs = creationTimeMs;
  }

  public synchronized LineageInfo generateLineageInfo() {
    LineageInfo info = new LineageInfo();
    info.mId = mId;
    List<Long> inputFiles = Lists.newArrayList();
    for (TachyonFile file : mInputFiles) {
      inputFiles.add(file.getFileId());
    }
    info.mInputFiles = inputFiles;

    List<LineageFileInfo> outputFiles = Lists.newArrayList();
    for (LineageFile lineageFile : mOutputFiles) {
      outputFiles.add(lineageFile.generateLineageFileInfo());
    }
    info.mOutputFiles = outputFiles;

    // TODO(yupeng) allow other types of jobs
    info.mJob = ((CommandLineJob) mJob).generateCommandLineJobInfo();
    info.mCreationTimeMs = mCreationTimeMs;
    return info;
  }

  public synchronized List<TachyonFile> getInputFiles() {
    return Collections.unmodifiableList(mInputFiles);
  }

  public synchronized List<LineageFile> getOutputFiles() {
    return Collections.unmodifiableList(mOutputFiles);
  }

  public Job getJob() {
    return mJob;
  }

  public long getId() {
    return mId;
  }

  public long getCreationTime() {
    return mCreationTimeMs;
  }

  public synchronized void recordOutputFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.COMPLETED);
      }
    }
  }

  public synchronized void addLostFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.LOST);
      }
    }
  }

  public synchronized boolean needRecompute() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.LOST) {
        return true;
      }
    }
    return false;
  }

  public synchronized boolean isCompleted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  public synchronized void commitOutputFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.PERSISTED);
      }
    }
  }

  public synchronized boolean isPersisted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.PERSISTED) {
        return false;
      }
    }
    return true;
  }

  public synchronized boolean isInCheckpointing() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.PERSISENCE_REQUESTED) {
        return true;
      }
    }
    return false;
  }

  public synchronized List<Long> getLostFiles() {
    List<Long> result = Lists.newArrayList();
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.LOST) {
        result.add(outputFile.getFileId());
      }
    }
    return result;
  }

  public synchronized LineageEntry toJournalEntry() {
    return new LineageEntry(mId, mInputFiles, mOutputFiles, mJob, mCreationTimeMs);
  }
}
