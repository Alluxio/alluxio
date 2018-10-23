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

import alluxio.master.journal.ufs.UfsJournalFileParser;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An interface to parse a journal file.
 */
@NotThreadSafe
public interface JournalFileParser extends Closeable {
  /**
   * Factory to create {@link JournalFileParser}.
   */
  final class Factory {
    private Factory() {}  // prevent instantiation

    /**
     * Creates a journal parser given the location.
     *
     * @param location the location to the journal file
     * @return the journal parser
     */
    public static JournalFileParser create(URI location) {
      return new UfsJournalFileParser(location);
    }
  }

  /**
   * Parses a journal file and returns the next entry from the journal file. Return null if there
   * is no more entry left.
   *
   * @return the journal entry, null if no more entry left
   */
  JournalEntry next() throws IOException;
}
