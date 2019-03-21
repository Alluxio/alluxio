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

package alluxio.master.journal.checkpoint;

import alluxio.master.journal.PatchedInputChunked;

import com.esotericsoftware.kryo.io.InputChunked;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Optional;

/**
 * Reader for compound checkpoints. Compound checkpoints follow the format
 *
 * [componentName1, componentBytes1, componentName2, componentBytes2, ...]
 *
 * The bytes are written using Kryo's chunked encoding.
 */
public class CompoundCheckpointReader {
  private final InputChunked mStream;
  private boolean mFirstCheckpoint = true;

  /**
   * @param in a checkpoint input stream to read from
   */
  public CompoundCheckpointReader(CheckpointInputStream in) {
    Preconditions.checkState(in.getType() == CheckpointType.COMPOUND,
        "Unexpected checkpoint type: %s", in.getType());
    mStream = new PatchedInputChunked(in);
  }

  /**
   * Returns an entry for the next checkpoint.
   *
   * The entry is only valid until nextCheckpoint is called again.
   *
   * @return the next checkpoint, or empty if there are no checkpoints remaining
   */
  public Optional<Entry> nextCheckpoint() throws IOException {
    // Skip calling nextChunks() the first time.
    if (mFirstCheckpoint) {
      mFirstCheckpoint = false;
    } else {
      mStream.nextChunks();
    }
    if (mStream.eof()) {
      return Optional.empty();
    }
    CheckpointName name = CheckpointName.valueOf(mStream.readString());
    CheckpointInputStream cis = new CheckpointInputStream(mStream);
    return Optional.of(new Entry(name, cis));
  }

  /**
   * A compound checkpoint entry.
   */
  public static class Entry {
    private final CheckpointName mName;
    private final CheckpointInputStream mStream;

    /**
     * @param name checkpoint name
     * @param stream checkpoint stream
     */
    public Entry(CheckpointName name, CheckpointInputStream stream) {
      mName = name;
      mStream = stream;
    }

    /**
     * @return the checkpoint name
     */
    public CheckpointName getName() {
      return mName;
    }

    /**
     * Return a checkpoint stream containing the bytes for this checkpoint.
     *
     * Callers should *not* close the returned stream, since it may contain additional checkpoint
     * data for other components.
     *
     * @return the checkpoint stream
     */
    public CheckpointInputStream getStream() {
      return mStream;
    }
  }
}
