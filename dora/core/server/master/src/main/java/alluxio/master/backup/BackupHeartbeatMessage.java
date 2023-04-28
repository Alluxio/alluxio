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

package alluxio.master.backup;

import alluxio.grpc.BackupPStatus;
import alluxio.wire.BackupStatus;

import com.google.common.base.MoreObjects;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

import javax.annotation.Nullable;

/**
 * The backup message used for sending status of current backup to leader.
 */
public class BackupHeartbeatMessage implements CatalystSerializable {
  /** Current backup status. */
  private BackupStatus mBackupStatus;

  /**
   * Empty constructor as per deserialization requirement.
   */
  public BackupHeartbeatMessage() {
    mBackupStatus = null;
  }

  /**
   * Creates a heartbeat message with given backup status.
   *
   * @param backupStatus the backup status
   */
  public BackupHeartbeatMessage(@Nullable BackupStatus backupStatus) {
    mBackupStatus = backupStatus;
  }

  /**
   * @return the nullable {@link BackupStatus} in this heartbeat
   */
  public BackupStatus getBackupStatus() {
    return mBackupStatus;
  }

  @Override
  public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {
    if (mBackupStatus == null) {
      bufferOutput.writeBoolean(false);
      return;
    }

    // Leverage {@link BackupStatus#toProto} for serialization.
    byte[] statusBytes = mBackupStatus.toProto().toByteArray();
    bufferOutput.writeBoolean(true);
    bufferOutput.writeInt(statusBytes.length);
    bufferOutput.writeBytes(statusBytes);
  }

  @Override
  public void readObject(BufferInput<?> bufferInput, Serializer serializer) {
    if (bufferInput.readBoolean()) {
      // Leverage {@link BackupStatus#fromProto} for deserialization.
      int statusBytesLen = bufferInput.readInt();
      byte[] statusBytes = bufferInput.readBytes(statusBytesLen);
      try {
        mBackupStatus = BackupStatus.fromProto(BackupPStatus.parseFrom(statusBytes));
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize BackupStatus message.", e);
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("BackupStatus", mBackupStatus)
        .toString();
  }
}
