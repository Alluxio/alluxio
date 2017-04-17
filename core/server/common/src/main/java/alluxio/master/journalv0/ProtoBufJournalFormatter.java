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

package alluxio.master.journalv0;

import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.proto.ProtoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Reads and writes protocol buffer journal entries. The entries contain headers describing their
 * length. This framing is handled entirely by {@link JournalEntry#writeDelimitedTo(OutputStream)}
 * and {@link JournalEntry#parseDelimitedFrom(InputStream)}. This class is thread-safe.
 */
@ThreadSafe
public final class ProtoBufJournalFormatter implements JournalFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoBufJournalFormatter.class);

  /**
   * Constructs a new {@link ProtoBufJournalFormatter}.
   */
  public ProtoBufJournalFormatter() {}

  @Override
  public void serialize(JournalEntry entry, OutputStream outputStream) throws IOException {
    entry.writeDelimitedTo(outputStream);
  }

  @Override
  public JournalInputStream deserialize(final InputStream inputStream) throws IOException {
    return new JournalInputStream() {
      private final byte[] mBuffer = new byte[1024];
      private long mLatestSequenceNumber;

      @Override
      public JournalEntry read() throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
          return null;
        }
        // All journal entries start with their size in bytes written as a varint.
        int size = ProtoUtils.readRawVarint32(firstByte, inputStream);
        byte[] buffer = size <= mBuffer.length ? mBuffer : new byte[size];
        // Total bytes read so far for journal entry.
        int totalBytesRead = 0;
        while (totalBytesRead < size) {
          // Bytes read in last read request.
          int latestBytesRead = inputStream.read(buffer, totalBytesRead, size - totalBytesRead);
          if (latestBytesRead < 0) {
            break;
          }
          totalBytesRead += latestBytesRead;
        }
        if (totalBytesRead < size) {
          LOG.warn("Journal entry was truncated. Expected to read " + size + " bytes but only got "
              + totalBytesRead);
          return null;
        }

        JournalEntry entry = JournalEntry.parseFrom(new ByteArrayInputStream(buffer, 0, size));
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
