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

import alluxio.master.journalv0.ufs.UfsMutableJournal;
import alluxio.util.URIUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The read-write journal. This allows both reads and writes to the journal.
 */
@ThreadSafe
public interface MutableJournal extends Journal {

  /**
   * A {@link MutableJournal} factory.
   */
  @ThreadSafe
  final class Factory implements JournalFactory {
    private final URI mBase;

    /**
     * Creates a read-write journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * @param base the base location for journals created by this factory
     */
    public Factory(URI base) {
      mBase = base;
    }

    @Override
    public MutableJournal create(String name) {
      try {
        return new UfsMutableJournal(URIUtils.appendPath(mBase, name));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Creates a new read-write journal using the given location.
     *
     * @param location the journal location
     * @return a new instance of {@link Journal}
     */
    public static MutableJournal create(URI location) {
      return new UfsMutableJournal(location);
    }
  }

  /**
   * Formats the journal.
   */
  void format() throws IOException;

  /**
   * @return the {@link JournalWriter} for this journal
   */
  JournalWriter getWriter();
}
