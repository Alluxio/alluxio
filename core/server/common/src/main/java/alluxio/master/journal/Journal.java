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

import alluxio.exception.status.UnavailableException;

import java.io.Closeable;
import java.net.URI;

/**
 * A journal for persisting journal entries.
 */
public interface Journal extends Closeable {
  /**
   * @return the journal location
   */
  URI getLocation();

  /**
   * @return a journal context for appending journal entries
   * @throws UnavailableException if a context cannot be created because the journal has been
   *         closed.
   */
  JournalContext createJournalContext() throws UnavailableException;
}
