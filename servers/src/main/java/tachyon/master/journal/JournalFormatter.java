/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.util.CommonUtils;

/**
 * This describes the interface for serializing and deserializing entries in the journal.
 */
public interface JournalFormatter {
  class Factory {
    /**
     * Factory method for {@link JournalFormatter}.
     *
     * @param conf TachyonConf to get the type of {@link JournalFormatter}
     * @return the created formatter
     */
    public static JournalFormatter create(TachyonConf conf) {
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
   * @throws IOException
   */
  void serialize(JournalEntry entry, OutputStream outputStream) throws IOException;

  /**
   * Returns a {@link JournalInputStream} from the given input stream. The returned input stream
   * produces a stream of {@link JournalEntry} objects.
   *
   * @param inputStream The input stream to deserialize
   * @return a {@link JournalInputStream} for reading all the journal entries in the given input
   *         stream.
   * @throws IOException
   */
  JournalInputStream deserialize(InputStream inputStream) throws IOException;
}
