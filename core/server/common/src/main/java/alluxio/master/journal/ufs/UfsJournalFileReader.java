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

package alluxio.master.journal.ufs;

import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} that reads journal entries from a journal file.
 */
@NotThreadSafe
final class UfsJournalFileReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UnderFileSystem mUfs;
  /** The next journal entry sequence number to read. */
  private long mNextSequenceNumber;
  /** Buffer used to read from the file. */
  private final byte[] mBuffer = new byte[1024];

  /** The input stream to read from the journal file. */
  private InputStream mInputStream;
  /** The location of the journal file. */
  private URI mLocation;

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   */
  UfsJournalFileReader(JournalReaderOptions options) {
    mNextSequenceNumber = -1;
    mLocation = Preconditions.checkNotNull(options.getLocation());
    mUfs = UnderFileSystem.Factory.get(mLocation.toString());
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public boolean shouldCheckpoint() throws IOException {
    return false;
  }

  @Override
  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  @Override
  public Journal.JournalEntry read() throws IOException, InvalidJournalEntryException {
    if (mInputStream == null) {
      mInputStream = mUfs.open(mLocation.toString());
    }

    int firstByte = mInputStream.read();
    if (firstByte == -1) {
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size;
    try {
      size = ProtoUtils.readRawVarint32(firstByte, mInputStream);
    } catch (IOException e) {
      LOG.warn("Journal entry was truncated in the size portion.");
      return null;
    }
    byte[] buffer = size <= mBuffer.length ? mBuffer : new byte[size];
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mInputStream.read(buffer, totalBytesRead, size - totalBytesRead);
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

    Journal.JournalEntry entry =
        Journal.JournalEntry.parseFrom(new ByteArrayInputStream(buffer, 0, size));
    mNextSequenceNumber = entry.getSequenceNumber() + 1;
    return entry;
  }
}
