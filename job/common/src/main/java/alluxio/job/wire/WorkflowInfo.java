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

import alluxio.job.ProtoUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The workflow description.
 */
public class WorkflowInfo implements JobInfo {

  private final long mId;
  private final Status mStatus;
  private final long mLastUpdated;
  private final List<JobInfo> mChildren;

  /**
   * Default constructor.
   * @param id id of the workflow
   * @param status {@link Status} of the workflow
   * @param lastUpdated lastUpdated time in milliseconds
   * @param children list of child job infos
   */
  public WorkflowInfo(long id, Status status, long lastUpdated,
      List<JobInfo> children) {
    mId = id;
    mStatus = status;
    mChildren = children;
    mLastUpdated = lastUpdated;
  }

  /**
   * Constructor from the proto object.
   * @param jobInfo proto representation of the job
   */
  public WorkflowInfo(alluxio.grpc.JobInfo jobInfo) throws IOException {
    Preconditions.checkArgument(jobInfo.getType().equals(JobType.WORKFLOW), "Invalid type");

    mId = jobInfo.getId();
    mStatus = Status.valueOf(jobInfo.getStatus().name());
    mLastUpdated = jobInfo.getLastUpdated();
    mChildren = Lists.newArrayList();
    for (alluxio.grpc.JobInfo childJobInfo : jobInfo.getChildrenList()) {
      mChildren.add(ProtoUtils.fromProto(childJobInfo));
    }
  }

  @Override
  public long getId() {
    return mId;
  }

  @Nullable
  @Override
  public Long getParentId() {
    return null;
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
    return mLastUpdated;
  }

  @Nonnull
  @Override
  public Collection<JobInfo> getChildren() {
    return Collections.unmodifiableList(mChildren);
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
    alluxio.grpc.JobInfo.Builder builder = alluxio.grpc.JobInfo.newBuilder().setId(mId)
        .setStatus(mStatus.toProto()).setLastUpdated(mLastUpdated).setType(JobType.WORKFLOW);

    for (JobInfo child : mChildren) {
      builder.addChildren(child.toProto());
    }

    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mStatus, mLastUpdated, mChildren);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId)
        .add("status", mStatus)
        .add("lastUpdated", mLastUpdated)
        .add("children", mChildren)
        .toString();
  }
}
