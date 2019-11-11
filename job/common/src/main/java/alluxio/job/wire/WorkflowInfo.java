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

import alluxio.grpc.JobType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * The workflow description.
 */
public class WorkflowInfo implements JobInfo {

  private final long mId;
  private final Long mParentId;
  private final Status mStatus;

  /**
   * Default constructor.
   * @param id id of the workflow
   * @param parentId parent id of the workflow
   * @param status {@link Status} of the workflow
   */
  public WorkflowInfo(long id, Long parentId, Status status) {
    mId = id;
    mParentId = parentId;
    mStatus = status;
  }

  /**
   * Constructor from the proto object.
   * @param jobInfo proto representation of the job
   */
  public WorkflowInfo(alluxio.grpc.JobInfo jobInfo) {
    Preconditions.checkArgument(jobInfo.getType().equals(JobType.WORKFLOW), "Invalid type");

    mId = jobInfo.getId();
    mParentId = jobInfo.getParentId();
    mStatus = Status.valueOf(jobInfo.getStatus().name());
  }

  @Override
  public long getId() {
    return mId;
  }

  @Nullable
  @Override
  public Long getParentId() {
    return mParentId;
  }

  @Nonnull
  @Override
  public String getName() {
    return "Workflow";
  }

  @Nonnull
  @Override
  public String getDescription() {
    return "";
  }

  @Nonnull
  @Override
  public Status getStatus() {
    return mStatus;
  }

  @Override
  public long getLastUpdated() {
    return 0;
  }

  @Nonnull
  @Override
  public Collection<JobInfo> getChildren() {
    return Lists.newArrayList();
  }

  @Nullable
  @Override
  public Serializable getResult() {
    return null;
  }

  @Nonnull
  @Override
  public String getErrorMessage() {
    return "";
  }

  @Nonnull
  @Override
  public alluxio.grpc.JobInfo toProto() throws IOException {
    return alluxio.grpc.JobInfo.newBuilder().setId(mId).setStatus(mStatus.toProto()).build();
  }
}
