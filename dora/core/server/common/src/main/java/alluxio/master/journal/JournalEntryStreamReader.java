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

package alluxio.master.journal;

import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.proto.ProtoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class for reading journal entries from an input stream.
 */
public class JournalEntryStreamReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(JournalEntryStreamReader.class);

  private final InputStream mStream;
  private byte[] mBuffer = new byte[4096];

  /**
   * @param stream the stream to read from
   */
  public JournalEntryStreamReader(InputStream stream) {
    mStream = stream;
  }

  /**
   * Reads a journal entry from the input stream.
   *
   * @return the journal entry, null if no journal entry is found
   */
  public JournalEntry readEntry() throws IOException {
    int firstByte = mStream.read();
    if (firstByte == -1) {
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size;
    try {
      size = ProtoUtils.readRawVarint32(firstByte, mStream);
    } catch (IOException e) {
      LOG.warn("Journal entry was truncated in the size portion.");
      throw e;
    }
    if (mBuffer.length < size) {
      mBuffer = new byte[size];
    }
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mStream.read(mBuffer, totalBytesRead, size - totalBytesRead);
      if (latestBytesRead < 0) {
        break;
      }
      totalBytesRead += latestBytesRead;
    }
    if (totalBytesRead < size) {
      // This could happen if the master crashed partway through writing the final journal entry. In
      // this case, we can ignore the last entry because it was not acked to the client.
      LOG.warn("Journal entry was truncated. Expected to read {} bytes but only got {}", size,
          totalBytesRead);
      return null;
    }
    return JournalEntry.parser().parseFrom(mBuffer, 0, size);
  }

  @Override
  public void close() throws IOException {
    mStream.close();
  }
}
