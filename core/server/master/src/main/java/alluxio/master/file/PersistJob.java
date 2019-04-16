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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.time.ExponentialTimer;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a persist job.
 */
@NotThreadSafe
public final class PersistJob {
  /** The id of the file that is persisted. */
  private final long mFileId;
  /** The URI of the file (NOTE: this can be out of date and should only be used for logging). */
  private final AlluxioURI mUri;
  /** The id of the persist job. */
  private final long mId;
  /** The temporary UFS path the file is persisted to. */
  private final String mTempUfsPath;
  /** The timer used for retrying failed jobs. */
  private final ExponentialTimer mTimer;
  /** The cancel state. */
  private CancelState mCancelState;

  /**
   * Represents the possible cancel states.
   */
  public enum CancelState {
    NOT_CANCELED,
    TO_BE_CANCELED,
    CANCELING,
  }

  /**
   * Creates a new instance of {@link PersistJob}.
   *
   * @param id the job id to use
   * @param fileId the file id to use
   * @param uri the file URI to use
   * @param tempUfsPath the temporary UFS path to use
   * @param timer the timer to use
   */
  public PersistJob(long id, long fileId, AlluxioURI uri, String tempUfsPath,
      ExponentialTimer timer) {
    mId = id;
    mFileId = fileId;
    mUri = uri;
    mTempUfsPath = tempUfsPath;
    mTimer = timer;
    mCancelState = CancelState.NOT_CANCELED;
  }

  /**
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
  }

  /**
   * @return the file uri
   */
  public AlluxioURI getUri() {
    return mUri;
  }

  /**
   * @return the job id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the temporary UFS path
   */
  public String getTempUfsPath() {
    return mTempUfsPath;
  }

  /**
   * @return the timer
   */
  public ExponentialTimer getTimer() {
    return mTimer;
  }

  /**
   * @return the {@link CancelState}
   */
  public CancelState getCancelState() {
    return mCancelState;
  }

  /**
   * @param cancelState the {@link CancelState} to set
   */
  public void setCancelState(CancelState cancelState) {
    mCancelState = cancelState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistJob)) {
      return false;
    }
    PersistJob that = (PersistJob) o;
    return Objects.equal(mFileId, that.mFileId)
        && Objects.equal(mUri, that.mUri)
        && Objects.equal(mId, that.mId)
        && Objects.equal(mTempUfsPath, that.mTempUfsPath)
        && Objects.equal(mTimer, that.mTimer)
        && Objects.equal(mCancelState, that.mCancelState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFileId, mUri, mId, mTempUfsPath, mTimer, mCancelState);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fileId", mFileId).add("uri", mUri).add("id", mId)
        .add("tempUfsPath", mTempUfsPath).add("timer", mTimer).add("cancelState", mCancelState)
        .toString();
  }
}
