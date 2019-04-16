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

import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A wrapper for giving an {@link OutputStream} API on top of Copycat's {@link SnapshotWriter}.
 */
public class SnapshotWriterStream extends OutputStream {
  private final SnapshotWriter mWriter;

  /**
   * @param writer the writer to wrap
   */
  public SnapshotWriterStream(SnapshotWriter writer) {
    mWriter = writer;
  }

  @Override
  public void write(int b) throws IOException {
    mWriter.writeByte(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    mWriter.write(b, off, len);
  }
}
