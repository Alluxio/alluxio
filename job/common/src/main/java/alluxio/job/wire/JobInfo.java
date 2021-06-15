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

package alluxio.job.wire;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * The Job Info. {@link JobInfo} can currently be either {@link TaskInfo}, {@link PlanInfo},
 * or {@link WorkflowInfo}.
 */
public interface JobInfo {

  /**
   * @return job id
   */
  long getId();

  /**
   * @return parent job id
   */
  @Nullable
  Long getParentId();

  /**
   * @return name of the job
   */
  @Nonnull
  String getName();

  /**
   * @return description of the job
   */
  @Nonnull
  String getDescription();

  /**
   * @return status of the job
   */
  @Nonnull
  Status getStatus();

  /**
   * @return last updated time in milliseconds
   */
  long getLastUpdated();

  /**
   * @return collection of children
   */
  @Nonnull
  List<JobInfo> getChildren();

  /**
   * @return result of the job
   */
  @Nullable
  Serializable getResult();

  /**
   * @return error type
   */
  @Nonnull
  String getErrorType();

  /**
   * @return error message
   */
  @Nonnull
  String getErrorMessage();

  /**
   * @return affected paths
   */
  @Nonnull
  default List<String> getAffectedPaths() {
    return Collections.emptyList();
  }

  /**
   * @return proto representation of the job info
   * @throws IOException if serialization fails
   */

  @Nonnull
  alluxio.grpc.JobInfo toProto() throws IOException;
}
