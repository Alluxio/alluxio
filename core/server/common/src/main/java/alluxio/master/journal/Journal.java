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

import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.master.journal.options.JournalWriterOptions;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.util.URIUtils;

import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A journal interface.
 */
public interface Journal {

  /**
   * A {@link Journal} factory.
   */
  @ThreadSafe
  final class Factory implements JournalFactory {
    private final URI mBase;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base location.
     *
     * @param base the base location for journals created by this factory
     */
    public Factory(URI base) {
      mBase = base;
    }

    @Override
    public Journal create(String name) {
      return new UfsJournal(URIUtils.appendPathOrDie(mBase, name));
    }
  }

  /**
   * @return the journal location
   */
  URI getLocation();

  /**
   * @param options the options to create the reader
   * @return the {@link JournalReader} for this journal
   */
  JournalReader getReader(JournalReaderOptions options);

  /**
   * @param options the options to create the writer
   * @return the {@link JournalWriter} for this journal
   */
  JournalWriter getWriter(JournalWriterOptions options) throws IOException;

  /**
   * Gets the log sequence number of the last journal entry written to checkpoint + 1.
   *
   * @return the next sequence number to checkpoint
   */
  long getNextSequenceNumberToCheckpoint() throws IOException;

  /**
   * @return whether the journal has been formatted
   */
  boolean isFormatted() throws IOException;

  /**
   * Formats the journal.
   */
  void format() throws IOException;
}
