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

package alluxio.coordinator;

import alluxio.Server;
import alluxio.exception.status.UnavailableException;
import alluxio.coordinator.journal.JournalContext;
import alluxio.coordinator.journal.Journaled;

/**
 * This interface contains common operations for all masters.
 */
public interface Master extends Journaled, Server<Boolean> {
  /**
   * @return a journal context for writing journal entries to the master
   */
  JournalContext createJournalContext() throws UnavailableException;

  /**
   * @return a master context
   */
  MasterContext getMasterContext();
}
