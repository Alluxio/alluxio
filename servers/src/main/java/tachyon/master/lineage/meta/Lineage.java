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

import tachyon.client.file.TachyonFile;
import tachyon.job.Job;

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
   * Creates a new lineage. The state will be initialized to ADDED.
   *
   * @param inputFiles the input files.
   * @param outputFiles the output files.
   * @param job the job
   */
  public Lineage(List<TachyonFile> inputFiles, List<LineageFile> outputFiles, Job job) {
    mInputFiles = Preconditions.checkNotNull(inputFiles);
    mOutputFiles = Preconditions.checkNotNull(outputFiles);
    mJob = Preconditions.checkNotNull(job);
    mId = LineageIdGenerator.generateId();
    mCreationTimeMs = System.currentTimeMillis();
  }

  public List<TachyonFile> getInputFiles() {
    return Collections.unmodifiableList(mInputFiles);
  }

  public List<LineageFile> getOutputFiles() {
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

  public void recordOutputFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.COMPLETED);
      }
    }
  }

  public void addLostFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.LOST);
      }
    }
  }

  public boolean needRecompute() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() == LineageFileState.LOST) {
        return true;
      }
    }
    return false;
  }

  public boolean isCompleted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  public void commitOutputFile(long fileId) {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getFileId() == fileId) {
        outputFile.setState(LineageFileState.PERSISTED);
      }
    }
  }

  public boolean isPersisted() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.PERSISTED) {
        return false;
      }
    }
    return true;
  }

  public boolean isInCheckpointing() {
    for (LineageFile outputFile : mOutputFiles) {
      if (outputFile.getState() != LineageFileState.PERSISENCE_REQUESTED) {
        return true;
      }
    }
    return false;
  }
}
