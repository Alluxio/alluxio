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

import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsMutableJournal;
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
   * A factory which creates read-write journals.
   */
  @ThreadSafe
  final class Factory {
    private final URI mBase;
    private final boolean mMutable;

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * @param base the base location for journals created by this factory
     */
    public Factory(URI base) {
      this(base, false);
    }

    /**
     * Creates a journal factory with the specified base location. When journals are
     * created, their names are appended to the base path.
     *
     * @param base the base location for journals created by this factory
     * @param mutable whether the journal is mutable or not
     */
    public Factory(URI base, boolean mutable) {
      mBase = base;
      mMutable = mutable;
    }

    /**
     * Creates a new instance of {@link Journal} using the given journal name.
     *
     * @param name the journal name
     * @return a new instance of {@link Journal}
     */
    public Journal create(String name) {
      try {
        if (!mMutable) {
          return new UfsJournal(URIUtils.appendPath(mBase, name));
        } else {
          return new UfsMutableJournal(URIUtils.appendPath(mBase, name));
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Creates a new instance of {@link Journal} using the given journal location.
     *
     * @param location the journal location
     * @return a new instance of {@link Journal}
     */
    public static Journal create(URI location) {
      return create(location, false);
    }

    /**
     * Creates a new instance of {@link Journal} using the given journal location.
     *
     * @param location the journal location
     * @oaram mutable whether the journal is mutable or not
     * @return a new instance of {@link Journal}
     */
    public static Journal create(URI location, boolean mutable) {
      if (!mutable) {
        return new UfsJournal(location);
      } else {
        return new UfsMutableJournal(location);
      }
    }
  }

  /**
   * Formats the journal.
   *
   * @return whether the format operation succeeded or not
   * @throws IOException if an I/O error occurs
   */
  boolean format() throws IOException;

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
   * @throws IOException if an I/O error occurs
   */
  boolean isFormatted() throws IOException;
}
