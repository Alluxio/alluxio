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

package alluxio.scheduler.job;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.job.JobDescription;
import alluxio.proto.journal.Journal;
import alluxio.wire.WorkerInfo;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * interface for job that can be scheduled by Alluxio scheduler.
 *
 * @param <T> the type of the task of the job
 */
public interface Job<T extends Task<?>> {

  /**
   * @return the job description, which is used to identify the intention of the job. We don't allow
   * multiple jobs with the same description to be run on the scheduler at the same time. When
   * submitting a job when there is already a job with the same description, scheduler will update
   * the job instead of submitting a new job.
   */
  JobDescription getDescription();

  /**
   * @return job end time if finished, otherwise empty
   */
  OptionalLong getEndTime();

  /**
   * @return whether the job need verification
   */
  boolean needVerification();

  /**
   * @return job state
   */
  JobState getJobState();

  /**
   * set job state.
   * @param state job state
   */
  void setJobState(JobState state);

  /**
   * @return job id. unique id for the job
   */
  String getJobId();

  /**
   * set job as failure with exception.
   * @param reason exception
   */
  void failJob(AlluxioRuntimeException reason);

  /**
   * set job as success.
   */
  void setJobSuccess();

  /**
   * Get job progress.
   * @param format progress report format
   * @param verbose whether to include detailed information
   * @return job progress report
   * @throws IllegalArgumentException if the format is not supported
   */
  String getProgress(JobProgressReportFormat format, boolean verbose);

  /**
   * Check whether the job is healthy.
   * @return true if the job is healthy, false if not
   */
  boolean isHealthy();

  /**
   * Check whether the job is still running.
   * @return true if the job is running, false if not
   */
  boolean isRunning();

  /**
   * Check whether the job is finished.
   * @return true if the job is finished, false if not
   */
  boolean isDone();

  /**
   * Check whether the current pass is finished.
   * @return true if the current pass of job is finished, false if not
   */
  boolean isCurrentPassDone();

  /**
   * Initiate a verification pass. This will re-list the directory and find
   * any unfinished files / tasks and try to execute them again.
   */
  void initiateVerification();

  /**
   * @param worker blocker to worker
   * @return the next task to run. If there is no more task to run, return empty
   * @throws AlluxioRuntimeException if any error occurs when getting next task
   */
  Optional<T> getNextTask(WorkerInfo worker);

  /**
   * @return job journal entry
   */
  Journal.JournalEntry toJournalEntry();

  /**
   * process task result.
   * @param task task containing result future
   * @return success if successfully process task result, otherwise return failure
   */
  boolean processResponse(T task);

  /**
   * update job configs.
   * @param job the job to update from. Must be the same job type
   */
  void updateJob(Job<?> job);
}
