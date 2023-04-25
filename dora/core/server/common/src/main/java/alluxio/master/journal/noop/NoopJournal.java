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

package alluxio.master.journal.noop;

import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournalContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Implementation of {@link Journal} that does nothing.
 */
public class NoopJournal implements Journal {

  /**
   * Creates a new instance of {@link NoopJournal}.
   */
  public NoopJournal() {}

  @Override
  public URI getLocation() {
    try {
      return new URI("/noop");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public JournalContext createJournalContext() {
    return NoopJournalContext.INSTANCE;
  }

  @Override
  public void close() throws IOException {}
}
