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

import alluxio.master.journal.ufs.ReadOnlyUfsJournal;
import alluxio.master.journal.ufs.ReadWriteUfsJournal;
import alluxio.master.journal.ufs.UfsJournal;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Interface for factories which create {@link UfsJournal}s.
 */
public interface JournalFactory {
  /**
   * @param name the name for the journal
   * @return a journal based on the given name
   */
  Journal get(String name);

  /**
   * A factory which creates read-write journals.
   */
  final class ReadWrite implements JournalFactory {
    private final URL mBaseLocation;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * Journals created by this factory support both reading and writing.
     *
     * @param baseLocation the base location for journals created by this factory
     */
    public ReadWrite(URL baseLocation) {
      mBaseLocation = baseLocation;
    }

    @Override
    public UfsJournal get(String name) {
      try {
        return new ReadWriteUfsJournal(new URL(mBaseLocation, name));
      } catch (MalformedURLException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  /**
   * A factory which creates read-only journals.
   */
  final class ReadOnly implements JournalFactory {
    private final URL mBaseLocation;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * Journals created by this factory only support reads.
     *
     * @param baseLocation the base location for journals created by this factory
     */
    public ReadOnly(URL baseLocation) {
      mBaseLocation = baseLocation;
    }

    @Override
    public UfsJournal get(String name) {
      try {
        return new ReadOnlyUfsJournal(new URL(mBaseLocation, name));
      } catch (MalformedURLException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }
}
