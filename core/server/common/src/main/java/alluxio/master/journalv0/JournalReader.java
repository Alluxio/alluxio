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

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class manages reading from the journal. The reading must occur in two phases:
 *
 * 1. First, the checkpoint must be read.
 *
 * 2. Afterwards, logs are read in order they were created. Only completed logs are read, so the
 * last log currently being written is not read until it is marked as complete.
 */
@NotThreadSafe
public interface JournalReader {

  /**
   * Checks to see if the journal checkpoint has not been updated. If it has been updated since the
   * creation of this reader, this reader is no longer valid.
   *
   * @return true if the checkpoint has not been updated
   */
  boolean isValid();

  /**
   * Gets the {@link JournalInputStream} for the journal checkpoint. This must be called before
   * calling {@link #getNextInputStream()}.
   *
   * @return the {@link JournalInputStream} for the journal checkpoint
   */
  JournalInputStream getCheckpointInputStream() throws IOException;

  /**
   * @return the input stream for the next completed log. Will return null if the next
   *         completed log does not exist yet.
   */
  JournalInputStream getNextInputStream() throws IOException;

  /**
   * @return the last modified time of the checkpoint in ms
   */
  long getCheckpointLastModifiedTimeMs() throws IOException;
}
