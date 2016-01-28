/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

import tachyon.proto.journal.Journal.JournalEntry;

/**
 * Reads and writes protocol buffer journal entries. The entries contain headers describing their
 * length. This framing is handled entirely by {@link JournalEntry#writeDelimitedTo(OutputStream)}
 * and {@link JournalEntry#parseDelimitedFrom(InputStream)}. This class is thread-safe.
 */
@ThreadSafe
public final class ProtoBufJournalFormatter implements JournalFormatter {
  @Override
  public void serialize(JournalEntry entry, OutputStream outputStream) throws IOException {
    entry.writeDelimitedTo(outputStream);
  }

  @Override
  public JournalInputStream deserialize(final InputStream inputStream) throws IOException {
    return new JournalInputStream() {
      private long mLatestSequenceNumber;

      @Override
      public JournalEntry getNextEntry() throws IOException {
        JournalEntry entry = JournalEntry.parseDelimitedFrom(inputStream);
        if (entry != null) {
          mLatestSequenceNumber = entry.getSequenceNumber();
        }
        return entry;
      }

      @Override
      public void close() throws IOException {
        inputStream.close();
      }

      @Override
      public long getLatestSequenceNumber() {
        return mLatestSequenceNumber;
      }
    };
  }
}
