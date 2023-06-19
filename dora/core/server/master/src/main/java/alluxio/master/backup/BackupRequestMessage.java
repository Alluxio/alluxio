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

import alluxio.grpc.BackupPRequest;

import com.google.common.base.MoreObjects;
import com.google.protobuf.InvalidProtocolBufferException;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The backup message used for signaling follower to start taking the backup.
 */
public class BackupRequestMessage implements CatalystSerializable {
  /** Client supplied backup request. */
  private BackupPRequest mBackupRequest;
  /** Unique backup id. */
  private UUID mBackupId;
  /** Consistent sequences of leader journals. */
  private Map<String, Long> mJournalSequences;

  /**
   * Empty constructor as per deserialization requirement.
   */
  public BackupRequestMessage() {}

  /**
   * Creates a backup request message.
   *
   * @param backupId the unique backup id
   * @param request client backup request
   * @param journalSequences consistent journal sequences
   */
  public BackupRequestMessage(UUID backupId, BackupPRequest request,
      Map<String, Long> journalSequences) {
    mBackupId = backupId;
    mBackupRequest = request;
    mJournalSequences = journalSequences;
  }

  /**
   * @return the unique backup id
   */
  public UUID getBackupId() {
    return mBackupId;
  }

  /**
   * @return the client backup request
   */
  public BackupPRequest getBackupRequest() {
    return mBackupRequest;
  }

  /**
   * @return the consistent set of journal sequences
   */
  public Map<String, Long> getJournalSequences() {
    return mJournalSequences;
  }

  @Override
  public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {
    bufferOutput.writeString(mBackupId.toString());
    byte[] serializedReq = mBackupRequest.toByteArray();
    bufferOutput.writeInt(serializedReq.length);
    bufferOutput.write(serializedReq);

    bufferOutput.writeInt(mJournalSequences.size());
    for (Map.Entry<String, Long> sequenceEntry : mJournalSequences.entrySet()) {
      bufferOutput.writeString(sequenceEntry.getKey());
      bufferOutput.writeLong(sequenceEntry.getValue());
    }
  }

  @Override
  public void readObject(BufferInput<?> bufferInput, Serializer serializer) {
    mBackupId = UUID.fromString(bufferInput.readString());
    int serializedReqLen = bufferInput.readInt();
    byte[] serializedReq = bufferInput.readBytes(serializedReqLen);
    try {
      mBackupRequest = BackupPRequest.parseFrom(serializedReq);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to deserialize backup request field.", e);
    }

    mJournalSequences = new HashMap<>();
    int entryCount = bufferInput.readInt();
    while (entryCount-- > 0) {
      mJournalSequences.put(bufferInput.readString(), bufferInput.readLong());
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("BackupId", mBackupId)
        .add("BackupRequest", mBackupRequest)
        .add("JournalSequences", mJournalSequences)
        .toString();
  }
}
