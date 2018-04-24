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

import alluxio.master.journalv0.ufs.UfsJournal;
import alluxio.util.URIUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The read-only journal. It prevents access to a {@link JournalWriter}.
 */
@ThreadSafe
public interface Journal {

  /**
   * A {@link Journal} factory.
   */
  @ThreadSafe
  final class Factory implements JournalFactory {
    private final URI mBase;

    /**
     * Creates a read-only journal factory with the specified base location. When journals are
     * created, their names are appended to the base location.
     *
     * @param base the base location for journals created by this factory
     */
    public Factory(URI base) {
      mBase = base;
    }

    @Override
    public Journal create(String name) {
      try {
        return new UfsJournal(URIUtils.appendPath(mBase, name));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Creates a new read-only journal using the given location.
     *
     * @param location the journal location
     * @return a new instance of {@link Journal}
     */
    public static Journal create(URI location) {
      return new UfsJournal(location);
    }
  }

  /**
   * @return the journal location
   */
  URI getLocation();

  /**
   * @return the {@link JournalReader} for this journal
   */
  JournalReader getReader();

  /**
   * @return whether the journal has been formatted
   */
  boolean isFormatted() throws IOException;
}
