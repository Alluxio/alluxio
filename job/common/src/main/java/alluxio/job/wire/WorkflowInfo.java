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
import java.util.Collections;
import java.util.List;

/**
 * The workflow description.
 */
public class WorkflowInfo implements JobInfo {

  private final long mId;
  private final String mName;
  private final Status mStatus;
  private final long mLastUpdated;
  private final String mErrorType;
  private final String mErrorMessage;
  private final List<JobInfo> mChildren;

  /**
   * Default constructor.
   * @param id id of the workflow
   * @param name name of the workflow
   * @param status {@link Status} of the workflow
   * @param lastUpdated lastUpdated time in milliseconds
   * @param errorType error type
   * @param errorMessage error message
   * @param children list of child job infos
   */
  public WorkflowInfo(long id, String name, Status status, long lastUpdated, String errorType,
                      String errorMessage, List<JobInfo> children) {
    mId = id;
    mName = name;
    mStatus = status;
    mLastUpdated = lastUpdated;
    mErrorType = (errorType == null) ? "" : errorType;
    mErrorMessage = (errorMessage == null) ? "" : errorMessage;
    mChildren = children;
  }

  /**
   * Constructor from the proto object.
   * @param jobInfo proto representation of the job
   */
  public WorkflowInfo(alluxio.grpc.JobInfo jobInfo) throws IOException {
    Preconditions.checkArgument(jobInfo.getType().equals(JobType.WORKFLOW), "Invalid type");

    mId = jobInfo.getId();
    mName = jobInfo.getName();
    mStatus = Status.valueOf(jobInfo.getStatus().name());
    mLastUpdated = jobInfo.getLastUpdated();
    mErrorType = jobInfo.getErrorType();
    mErrorMessage = jobInfo.getErrorMessage();
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
    return mName;
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
  public List<JobInfo> getChildren() {
    return Collections.unmodifiableList(mChildren);
  }

  @Nullable
  @Override
  public Serializable getResult() {
    return null;
  }

  @Nonnull
  @Override
  public String getErrorType() {
    return mErrorType;
  }

  @Nonnull
  @Override
  public String getErrorMessage() {
    return mErrorMessage;
  }

  @Nonnull
  @Override
  public alluxio.grpc.JobInfo toProto() throws IOException {
    alluxio.grpc.JobInfo.Builder builder = alluxio.grpc.JobInfo.newBuilder().setId(mId)
        .setName(mName).setStatus(mStatus.toProto()).setLastUpdated(mLastUpdated)
        .setErrorType(mErrorType).setErrorMessage(mErrorMessage).setType(JobType.WORKFLOW);

    for (JobInfo child : mChildren) {
      builder.addChildren(child.toProto());
    }

    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mStatus, mLastUpdated, mErrorType, mErrorMessage, mChildren);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkflowInfo)) {
      return false;
    }
    WorkflowInfo that = (WorkflowInfo) o;
    return Objects.equal(mId, that.mId)
        && Objects.equal(mChildren, that.mChildren)
        && Objects.equal(mStatus, that.mStatus)
        && Objects.equal(mLastUpdated, that.mLastUpdated)
        && Objects.equal(mErrorType, that.mErrorType)
        && Objects.equal(mErrorMessage, that.mErrorMessage);
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
