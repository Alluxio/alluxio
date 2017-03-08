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

import alluxio.master.journal.ufs.UfsReadOnlyJournal;
import alluxio.master.journal.ufs.UfsReadWriteJournal;
import alluxio.util.URIUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Interface for factories which create {@link Journal}s.
 */
public interface JournalFactory {
  /**
   * @param name the journal name
   * @return a journal based on the given name
   */
  Journal create(String name);

  /**
   * A factory which creates read-write journals.
   */
  final class ReadWrite implements JournalFactory {
    private final URI mBaseLocation;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * Journals created by this factory support both reading and writing.
     *
     * @param baseLocation the base location for journals created by this factory
     */
    public ReadWrite(URI baseLocation) {
      mBaseLocation = baseLocation;
    }

    @Override
    public Journal create(String name) {
      try {
        return new UfsReadWriteJournal(URIUtils.appendPath(mBaseLocation, name));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    public static Journal create(URI location) {
      return new UfsReadWriteJournal(location);
    }
  }

  /**
   * A factory which creates read-only journals.
   */
  final class ReadOnly implements JournalFactory {
    private final URI mBaseLocation;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * Journals created by this factory only support reading.
     *
     * @param baseLocation the base location for journals created by this factory
     */
    public ReadOnly(URI baseLocation) {
      mBaseLocation = baseLocation;
    }

    @Override
    public Journal create(String name) {
      try {
        return new UfsReadOnlyJournal(URIUtils.appendPath(mBaseLocation, name));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    public static Journal create(URI location) {
      return new UfsReadOnlyJournal(location);
    }
  }
}
