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

import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalFileParser;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalFileParser} that parses a journal file.
 */
@NotThreadSafe
public final class UfsJournalFileParser implements JournalFileParser {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalFileParser.class);

  private final UnderFileSystem mUfs;
  /** Buffer used to read from the file. */
  private byte[] mBuffer = new byte[1024];

  /** The input stream to read from the journal file. */
  private InputStream mInputStream;
  /** The location of the journal file. */
  private URI mLocation;

  /**
   * Creates a new instance of {@link UfsJournalFileParser}.
   *
   * @param location the journal file location
   */
  public UfsJournalFileParser(URI location) {
    mLocation = Preconditions.checkNotNull(location, "location");
    mUfs = UnderFileSystem.Factory.create(mLocation.toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public Journal.JournalEntry next() throws IOException {
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
    if (size > mBuffer.length) {
      mBuffer = new byte[size];
    }
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mInputStream.read(mBuffer, totalBytesRead, size - totalBytesRead);
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

    return Journal.JournalEntry.parser().parseFrom(mBuffer, 0, size);
  }
}
