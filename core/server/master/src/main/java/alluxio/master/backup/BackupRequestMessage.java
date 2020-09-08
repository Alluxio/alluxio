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
import alluxio.master.transport.serializer.MessagingSerializable;
import alluxio.master.transport.serializer.SerializerUtils;

import com.google.common.base.MoreObjects;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The backup message used for signaling follower to start taking the backup.
 */
public class BackupRequestMessage implements MessagingSerializable {
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
  public void writeObject(DataOutputStream os) throws IOException {
    SerializerUtils.writeStringToStream(os, mBackupId.toString());
    byte[] serializedReq = mBackupRequest.toByteArray();
    os.writeInt(serializedReq.length);
    os.write(serializedReq);

    os.writeInt(mJournalSequences.size());
    for (Map.Entry<String, Long> sequenceEntry : mJournalSequences.entrySet()) {
      SerializerUtils.writeStringToStream(os, sequenceEntry.getKey());
      os.writeLong(sequenceEntry.getValue());
    }
  }

  @Override
  public void readObject(DataInputStream is) throws IOException {
    mBackupId = UUID.fromString(SerializerUtils.readStringFromStream(is));
    byte[] serializedReq = SerializerUtils.readBytesFromStream(is);
    try {
      mBackupRequest = BackupPRequest.parseFrom(serializedReq);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to deserialize backup request field.", e);
    }

    mJournalSequences = new HashMap<>();
    int entryCount = is.readInt();
    while (entryCount-- > 0) {
      mJournalSequences.put(SerializerUtils.readStringFromStream(is), is.readLong());
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
