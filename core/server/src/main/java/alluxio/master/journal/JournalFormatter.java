/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This describes the interface for serializing and deserializing entries in the journal.
 */
public interface JournalFormatter {

  /**
   * Factory for {@link JournalFormatter}.
   */
  class Factory {
    /**
     * Factory method for {@link JournalFormatter}.
     *
     * @param conf Alluxio configuration to get the type of {@link JournalFormatter}
     * @return the created formatter
     */
    public static JournalFormatter create(Configuration conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<JournalFormatter>getClass(Constants.MASTER_JOURNAL_FORMATTER_CLASS), null, null);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Serializes the given entry and writes it to the given output stream.
   *
   * @param entry The journal entry to serialize
   * @param outputStream the output stream to serialize the entry to
   * @throws IOException if a non-Alluxio related exception occurs
   */
  void serialize(JournalEntry entry, OutputStream outputStream) throws IOException;

  /**
   * Returns a {@link JournalInputStream} from the given input stream. The returned input stream
   * produces a stream of {@link JournalEntry} objects.
   *
   * @param inputStream The input stream to deserialize
   * @return a {@link JournalInputStream} for reading all the journal entries in the given input
   *         stream.
   * @throws IOException if a non-Alluxio related exception occurs
   */
  JournalInputStream deserialize(InputStream inputStream) throws IOException;
}
