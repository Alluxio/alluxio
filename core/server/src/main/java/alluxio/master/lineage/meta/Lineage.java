/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.lineage.meta;

import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.job.JobConf;
import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Lineage.LineageEntry;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A lineage tracks the dependencies imposed by a job, including the input files the job depends on,
 * and the output files the job generates.
 */
@NotThreadSafe
public final class Lineage implements JournalEntryRepresentable {
  private final long mId;
  private final List<Long> mInputFiles;
  private final List<Long> mOutputFiles;
  private final Job mJob;
  private final long mCreationTimeMs;

  /**
   * Creates a new instance of {@link Lineage}.
   *
   * @param id the lineage id
   * @param inputFiles the input file ids
   * @param outputFiles the output file ids
   * @param job the job
   */
  public Lineage(long id, List<Long> inputFiles, List<Long> outputFiles, Job job) {
    this(id, inputFiles, outputFiles, job, System.currentTimeMillis());
  }

  /**
   * Creates a new instance of {@link Lineage}.
   *
   * This method should only used by lineage. TODO(yupeng): hide this method
   *
   * @param id the lineage id
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   * @param creationTimeMs the creation time
   */
  public Lineage(long id, List<Long> inputFiles, List<Long> outputFiles, Job job,
      long creationTimeMs) {
    mInputFiles = Preconditions.checkNotNull(inputFiles);
    mOutputFiles = Preconditions.checkNotNull(outputFiles);
    mJob = Preconditions.checkNotNull(job);
    mId = id;
    mCreationTimeMs = creationTimeMs;
  }

  /**
   * @return the input file ids
   */
  public synchronized List<Long> getInputFiles() {
    return Collections.unmodifiableList(mInputFiles);
  }

  /**
   * @return the output file ids
   */
  public synchronized List<Long> getOutputFiles() {
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
   * Converts the entry to a {@link Lineage}.
   *
   * @param entry the entry to convert
   * @return the {@link Lineage} representation
   */
  public static Lineage fromJournalEntry(LineageEntry entry) {
    List<Long> inputFiles = Lists.newArrayList(entry.getInputFilesList());

    List<Long> outputFiles = Lists.newArrayList();
    Job job = new CommandLineJob(entry.getJobCommand(), new JobConf(entry.getJobOutputPath()));

    return new Lineage(entry.getId(), inputFiles, outputFiles, job, entry.getCreationTimeMs());
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    List<Long> inputFileIds = Lists.newArrayList(mInputFiles);
    List<Long> outputFileIds = Lists.newArrayList(mOutputFiles);
    Preconditions.checkState(mJob instanceof CommandLineJob);
    CommandLineJob commandLineJob = (CommandLineJob) mJob;
    String jobCommand = commandLineJob.getCommand();
    String jobOutputPath = commandLineJob.getJobConf().getOutputFilePath();

    LineageEntry lineage = LineageEntry.newBuilder()
        .setId(mId)
        .addAllInputFiles(inputFileIds)
        .addAllOutputFileIds(outputFileIds)
        .setJobCommand(jobCommand)
        .setJobOutputPath(jobOutputPath)
        .setCreationTimeMs(mCreationTimeMs)
        .build();
    return JournalEntry.newBuilder().setLineage(lineage).build();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("inputFiles", mInputFiles)
        .add("outputFiles", mOutputFiles).add("job", mJob).add("id", mId)
        .add("creationTimeMs", mCreationTimeMs).toString();
  }
}
