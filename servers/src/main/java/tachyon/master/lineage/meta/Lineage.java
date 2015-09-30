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
import tachyon.master.journal.JournalEntryRepresentable;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.thrift.LineageFileInfo;
import tachyon.thrift.LineageInfo;

/**
 * A lineage tracks the dependencies imposed by a job, including the input files the job depends on,
 * and the output files the job generates.
 */
public final class Lineage implements JournalEntryRepresentable {
  private final long mId;
  private final List<TachyonFile> mInputFiles;
  private final List<LineageFile> mOutputFiles;
  private final Job mJob;
  private final long mCreationTimeMs;

  /**
   * Creates a new lineage.
   *
   * @param id the lineage id
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   */
  public Lineage(long id, List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job) {
    this(id, inputFiles, outputFiles, job, System.currentTimeMillis());
  }

  /**
   * A method for lineage only. TODO(yupeng): hide this method
   *
   * @param id the lineage id
   * @param inputFiles the input files
   * @param inputFiles the input files
   * @param outputFiles the output files
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

  /**
   * @return the {@link LineageInfo} for RPC
   */
  public synchronized LineageInfo generateLineageInfo() {
    LineageInfo info = new LineageInfo();
    info.id = mId;
    List<Long> inputFiles = Lists.newArrayList();
    for (TachyonFile file : mInputFiles) {
      inputFiles.add(file.getFileId());
    }
    info.inputFiles = inputFiles;

    List<LineageFileInfo> outputFiles = Lists.newArrayList();
    for (LineageFile lineageFile : mOutputFiles) {
      outputFiles.add(lineageFile.generateLineageFileInfo());
    }
    info.outputFiles = outputFiles;

    // TODO(yupeng) allow other types of jobs
    info.job = ((CommandLineJob) mJob).generateCommandLineJobInfo();
    info.creationTimeMs = mCreationTimeMs;
    return info;
  }

  /**
   * @return the input files.
   */
  public synchronized List<TachyonFile> getInputFiles() {
    return Collections.unmodifiableList(mInputFiles);
  }

  /**
   * @return the output files
   */
  public synchronized List<LineageFile> getOutputFiles() {
    return Collections.unmodifiableList(mOutputFiles);
  }

  /**
   * @return the job
   */
  public Job getJob() {
    return mJob;
  }

  /**
   * @return the lineage id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the creation time
   */
  public long getCreationTime() {
    return mCreationTimeMs;
  }

  /**
   * Updates the state of the lineage's output file.
   *
   * @param fileId the id of the output file
   * @param newState the new state
   */
  public synchronized void updateOutputFileState(long fileId, LineageFileState newState) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(newState);
        return;
      }
    }
  }

  /**
   * @return true if the lineage needs recompute, false otherwise
   */
  public synchronized boolean needRecompute() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.LOST) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if all the output files are completed, false otherwise
   */
  public synchronized boolean isCompleted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if all the output files are persisted, false otherwise
   */
  public synchronized boolean isPersisted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.PERSISTED) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if at least one of the output files is being persisted, false otherwise
   */
  public synchronized boolean isInCheckpointing() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.PERSISENCE_REQUESTED) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return all the output files that are lost on the workers.
   */
  public synchronized List<Long> getLostFiles() {
    List<Long> result = Lists.newArrayList();
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.LOST) {
        result.add(outputFile.getFileId());
      }
    }
    return result;
  }

  /**
   * Gets the state of the lineage's output file.
   *
   * @param fileId the id of the output file
   * @return the output file state
   */
  public synchronized LineageFileState getOutputFileState(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        return outputFile.getState();
      }
    }
    throw new RuntimeException("Output file " + fileId + " not found");
  }

  @Override
  public synchronized LineageEntry toJournalEntry() {
    return new LineageEntry(mId, mInputFiles, mOutputFiles, mJob, mCreationTimeMs);
  }

  @Override
  public String toString() {
    return generateLineageInfo().toString();
  }
}
