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
import alluxio.master.transport.serializer.SerializerUtils;
import alluxio.wire.BackupStatus;
import alluxio.master.transport.serializer.MessagingSerializable;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The backup message used for sending status of current backup to leader.
 */
public class BackupHeartbeatMessage implements MessagingSerializable {

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
  public void writeObject(DataOutputStream os) throws IOException {
    if (mBackupStatus == null) {
      os.writeBoolean(false);
      return;
    }

    // Leverage {@link BackupStatus#toProto} for serialization.
    byte[] statusBytes = mBackupStatus.toProto().toByteArray();
    os.writeBoolean(true);
    os.writeInt(statusBytes.length);
    os.write(statusBytes);
  }

  @Override
  public void readObject(DataInputStream is) throws IOException {
    if (is.readBoolean()) {
      // Leverage {@link BackupStatus#fromProto} for deserialization.
      int statusBytesLen = is.readInt();
      byte[] statusBytes = SerializerUtils.readBytesFromStream(is, statusBytesLen);
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
