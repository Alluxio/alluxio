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

import io.atomix.copycat.server.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream for reading from a {@link SnapshotReader}.
 */
public class SnapshotReaderStream extends InputStream {
  private final SnapshotReader mReader;

  /**
   * @param reader the reader to read from; note that this stream will never close the reader
   */
  public SnapshotReaderStream(SnapshotReader reader) {
    mReader = reader;
  }

  @Override
  public int read() throws IOException {
    if (!mReader.hasRemaining()) {
      return -1;
    }
    // readByte returns a signed byte cast as an int, but InputStream expects bytes to be
    // represented as ints in the range 0 to 255.
    return Byte.toUnsignedInt((byte) mReader.readByte());
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (!mReader.hasRemaining()) {
      return -1;
    }
    int read = (int) Math.min(len, mReader.remaining());
    mReader.read(b, off, read);
    return read;
  }
}
