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

package alluxio.master.journal.raft;

import alluxio.proto.journal.Journal.JournalEntry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;

/**
 * Command for writing an arbitrary journal entry.
 *
 * Journal entries are serialized by writing their size as an integer, followed by their bytes
 * serialized in protocol buffer format.
 */
public class JournalEntryCommand implements Command<Void>, CatalystSerializable {
  private static final long serialVersionUID = 7020023290825215903L;

  private int mSize;
  private byte[] mSerializedJournalEntry;

  /**
   * No-arg constructor for use by Catalyst deserialization.
   */
  public JournalEntryCommand() {}

  /**
   * Constructs a {@link JournalEntryCommand} for a journal entry.
   *
   * @param entry the journal entry
   */
  public JournalEntryCommand(JournalEntry entry) {
    mSize = entry.getSerializedSize();
    mSerializedJournalEntry = entry.toByteArray();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeInt(mSize);
    buffer.write(mSerializedJournalEntry);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    mSize = buffer.readInt();
    mSerializedJournalEntry = new byte[mSize];
    buffer.read(mSerializedJournalEntry);
  }

  /**
   * @return the serialized size of the journal entry
   */
  public int getSize() {
    return mSize;
  }

  /**
   * @return the serialized journal entry
   */
  public byte[] getSerializedJournalEntry() {
    return mSerializedJournalEntry;
  }
}
