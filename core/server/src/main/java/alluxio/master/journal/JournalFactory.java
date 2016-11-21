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

import alluxio.util.io.PathUtils;

/**
 * Interface for factories which create {@link Journal}s.
 */
public interface JournalFactory {
  /**
   * @param directory the directory for the journal
   * @return a journal based on the given directory
   */
  Journal get(String directory);

  abstract class AbstractJournalFactory implements JournalFactory {
    protected final String mBaseDirectory;

    /**
     * Creates a journal factory with the specified directory as the root. When journals are
     * created, their paths are appended to the base path, e.g.
     *
     * basesDirectory journalDirectory1 journalDirectory2
     *
     * @param baseDirectory the base directory for journals created by this factory
     */
    public AbstractJournalFactory(String baseDirectory) {
      mBaseDirectory = baseDirectory;
    }
  }

  final class ReadWrite extends AbstractJournalFactory {
    /**
     * {@inheritDoc}
     *
     * @param baseDirectory the base directory for journals created by this factory
     */
    public ReadWrite(String baseDirectory) {
      super(baseDirectory);
    }

    @Override
    public Journal get(String directory) {
      return new ReadWriteJournal(PathUtils.concatPath(mBaseDirectory, directory));
    }
  }

  final class ReadOnly extends AbstractJournalFactory {
    /**
     * {@inheritDoc}
     *
     * @param baseDirectory the base directory for journals created by this factory
     */
    public ReadOnly(String baseDirectory) {
      super(baseDirectory);
    }

    @Override
    public Journal get(String directory) {
      return new ReadWriteJournal(PathUtils.concatPath(mBaseDirectory, directory));
    }
  }
}
