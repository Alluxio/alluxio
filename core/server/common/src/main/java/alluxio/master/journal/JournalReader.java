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

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class manages reading from the journal.
 */
@NotThreadSafe
public interface JournalReader extends Closeable {
  /**
   * Returns the next entry in the journal.
   *
   * This will be null unless the last call to {@link #advance()} returned {@link State#LOG}.
   * Multiple calls to getEntry without calling advance will return the same entry.
   *
   * @return the journal entry, or null if the next element isn't a journal entry
   */
  JournalEntry getEntry();

  /**
   * Returns the input stream for reading a checkpoint.
   *
   * This will be null unless the last call to {@link #advance()} returned {@link State#CHECKPOINT}.
   * Multiple calls to getCheckpoint without calling advance will return the same stream.
   *
   * @return the input stream for reading the checkpoint, or null if the next element isn't a
   *         checkpoint
   */
  CheckpointInputStream getCheckpoint();

  /**
   * Gets the the sequence number of the next journal log entry to read. This method is valid
   * no matter whether this JournalReader is closed or not.
   *
   * @return the next sequence number
   */
  long getNextSequenceNumber();

  /**
   * Advances the reader to the next element.
   *
   * @return the next element, see {@link State}
   */
  State advance() throws IOException;

  /**
   * States that the reader can be after calling {@link #advance()}.
   */
  enum State {
    /**
     * Indicates that the next item to process is a checkpoint. The caller should call
     * {@link #getCheckpoint()}.
     */
    CHECKPOINT,
    /**
     * Indicates that the next item to process is an edit log. The caller should call
     * {@link #getEntry()}.
     */
    LOG,
    /**
     * Indicates that there is nothing left to read.
     */
    DONE
  }
}
