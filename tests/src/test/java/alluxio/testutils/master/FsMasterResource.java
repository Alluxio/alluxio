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

package alluxio.testutils.master;

import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * This class is a closeable resource that holds the master registry and the journal system. The
 * close() must be called to stop the masters and journal.
 */
public class FsMasterResource implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FsMasterResource.class);

  private final MasterRegistry mRegistry;
  private final JournalSystem mJournal;

  /**
   * @param registry the master registry
   * @param journal the journal system
   */
  FsMasterResource(MasterRegistry registry, JournalSystem journal) {
    mRegistry = registry;
    mJournal = journal;
  }

  /**
   * @return the master registry
   */
  public MasterRegistry getRegistry() {
    return mRegistry;
  }

  /**
   * @return the journal system
   */
  public JournalSystem getJournal() {
    return mJournal;
  }

  @Override
  public void close() throws IOException {
    try {
      mRegistry.close();
      mJournal.stop();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
