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

package alluxio.wire;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.grpc.BackupPStatus;
import alluxio.grpc.BackupState;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;

import java.util.UUID;

/**
 * Defines the status of a backup.
 */
public class BackupStatus {
  /** Unique identifier for the backup.  */
  private UUID mBackupId;
  /** State of backup. */
  private BackupState mState;
  /** Host that is taking the backup. */
  private String mBackupHost;
  /** URI where the backup is located. */
  private AlluxioURI mBackupUri;
  /** Counter for how many entries are written in a backup. */
  private long mEntryCount;
  /** The error if backup failed. */
  private AlluxioException mError;

  /**
   * Creates a new backup status with new Id.
   *
   * @param state state of the backup
   */
  public BackupStatus(BackupState state) {
    mBackupId = UUID.randomUUID();
    mState = state;
  }

  /**
   * Creates a new backup status with existing Id.
   *
   * @param backupId the backup id
   * @param state state of the backup
   */
  public BackupStatus(UUID backupId, BackupState state) {
    mBackupId = backupId;
    mState = state;
  }

  /**
   * Cloning constructor.
   *
   * @param status backup status to clone
   */
  public BackupStatus(BackupStatus status) {
    mBackupId = status.mBackupId;
    mState = status.mState;
    mBackupHost = status.mBackupHost;
    mBackupUri = status.mBackupUri;
    mEntryCount = status.mEntryCount;
    mError = status.mError;
  }

  /**
   * @param pStatus proto backup status
   * @return backup status from proto representation
   */
  public static BackupStatus fromProto(BackupPStatus pStatus) {
    BackupStatus status = new BackupStatus(pStatus.getBackupState());
    status.setBackupId(UUID.fromString(pStatus.getBackupId()));
    if (pStatus.hasBackupHost()) {
      status.setHostname(pStatus.getBackupHost());
    }
    if (pStatus.hasBackupUri()) {
      status.setBackupUri(new AlluxioURI(pStatus.getBackupUri()));
    }
    if (pStatus.hasBackupError()) {
      status.setError((AlluxioException) SerializationUtils
          .deserialize(pStatus.getBackupError().toByteArray()));
    }
    status.setEntryCount(pStatus.getEntryCount());
    return status;
  }

  /**
   * @return the unique backup id
   */
  public UUID getBackupId() {
    return mBackupId;
  }

  /**
   * Sets the unique backup id.
   *
   * @param backupUuid backup id
   */
  public void setBackupId(UUID backupUuid) {
    mBackupId = backupUuid;
  }

  /**
   * @return current entry count in the backup
   */
  public long getEntryCount() {
    return mEntryCount;
  }

  /**
   * Sets entry count.
   *
   * @param entryCount the entry count
   * @return the updated instance
   */
  public BackupStatus setEntryCount(long entryCount) {
    mEntryCount = entryCount;
    return this;
  }

  /**
   * @return {@code true} if the backup is finished (completed | failed)
   */
  public boolean isFinished() {
    return isCompleted() || isFailed();
  }

  /**
   * @return {@code true} if the backup is completed successfully
   */
  public boolean isCompleted() {
    return mState == BackupState.Completed;
  }

  /**
   * @return {@code true} if the backup is failed
   */
  public boolean isFailed() {
    return mState == BackupState.Failed;
  }

  /**
   * @return the backup state
   */
  public BackupState getState() {
    return mState;
  }

  /**
   * Sets the backup state.
   *
   * @param state the backup state
   * @return the updated instance
   */
  public BackupStatus setState(BackupState state) {
    mState = state;
    return this;
  }

  /**
   * @return the error if backup was failed
   */
  public AlluxioException getError() {
    return mError;
  }

  /**
   * Fails the backup with given error.
   * Adjusts the backup state to {@link BackupState#Failed}.
   *
   * @param error the backup error
   * @return the updated instance
   */
  public BackupStatus setError(AlluxioException error) {
    mError = error;
    mState = BackupState.Failed;
    return this;
  }

  /**
   * @return the backup host
   */
  public String getHostname() {
    return mBackupHost;
  }

  /**
   * Sets the backup host.
   *
   * @param hostName the backup host
   * @return the updated instance
   */
  public BackupStatus setHostname(String hostName) {
    mBackupHost = hostName;
    return this;
  }

  /**
   * @return the backup URI
   */
  public AlluxioURI getBackupUri() {
    return mBackupUri;
  }

  /**
   * Sets the backup uri.
   *
   * @param backupUri the backup uri
   * @return the updated instance
   */
  public BackupStatus setBackupUri(AlluxioURI backupUri) {
    mBackupUri = backupUri;
    return this;
  }

  /**
   * @return the proto representation
   */
  public BackupPStatus toProto() {
    BackupPStatus.Builder builder = BackupPStatus.newBuilder()
        .setBackupId(mBackupId.toString())
        .setBackupState(mState)
        .setEntryCount(mEntryCount);

    if (mBackupHost != null) {
      builder.setBackupHost(mBackupHost);
    }

    if (mError != null) {
      builder.setBackupError(ByteString.copyFrom(SerializationUtils.serialize(mError)));
    }

    if (mBackupUri != null) {
      builder.setBackupUri(mBackupUri.toString());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).omitNullValues()
        .add("BackupId", mBackupId)
        .add("BackupState", mState.name())
        .add("BackupHost", mBackupHost)
        .add("BackupURI", mBackupUri)
        .add("EntryCount", mEntryCount)
        .add("Error", mError)
        .toString();
  }
}
