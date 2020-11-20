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

import alluxio.master.transport.GrpcMessagingConnection;

import com.google.common.base.MoreObjects;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * The backup message used for introducing backup-worker to leader.
 */
public class BackupHandshakeMessage implements CatalystSerializable {
  /** Backup-worker's hostname. */
  private String mBackupWorkerHostname;

  /** Used to attach connection to message when arrived at the target. */
  private GrpcMessagingConnection mConnection;

  /**
   * Empty constructor as per deserialization requirement.
   */
  public BackupHandshakeMessage() {
    mBackupWorkerHostname = null;
  }

  /**
   * Creates a handshake message.
   *
   * @param hostname backup-worker hostname
   */
  public BackupHandshakeMessage(String hostname) {
    mBackupWorkerHostname = hostname;
  }

  /**
   * @return the hostname for the backup-worker
   */
  public String getBackupWorkerHostname() {
    return mBackupWorkerHostname;
  }

  /**
   * @param connection connection from which this message is coming from
   */
  public void setConnection(GrpcMessagingConnection connection) {
    mConnection = connection;
  }

  /**
   * @return connection from which this message is coming from
   */
  public GrpcMessagingConnection getConnection() {
    return mConnection;
  }

  @Override
  public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {
    bufferOutput.writeString(mBackupWorkerHostname);
  }

  @Override
  public void readObject(BufferInput<?> bufferInput, Serializer serializer) {
    mBackupWorkerHostname = bufferInput.readString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("BackupWorkerHostName", mBackupWorkerHostname)
        .toString();
  }
}
