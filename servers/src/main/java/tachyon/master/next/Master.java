/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next;

import org.apache.thrift.TProcessor;

import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalSerializable;

public interface Master extends JournalSerializable {
  /**
   * Returns the thrift processor for this master.
   *
   * @return the {@link TProcessor} serving this master.
   */
  TProcessor getProcessor();

  /**
   * Returns the service name for this master.
   *
   * @return a {@link String} representing this master service name.
   */
  String getProcessorName();

  /**
   * Processes the journal checkpoint file.
   *
   * @param inputStream the input stream for the journal checkpoint file.
   */
  void processJournalCheckpoint(JournalInputStream inputStream);

  /**
   * Processes a journal entry. These entries follow the checkpoint entries.
   *
   * @param entry the entry to process to update the state of the master.
   */
  void processJournalEntry(JournalEntry entry);

  /**
   * Start the master, as the leader master or standby. Here, the master should initialize state and
   * possibly start threads required for operation.
   *
   * @param asMaster if true, the master should behave as the leader master in the system. If false,
   *        the master should act as a standby master.
   */
  void start(boolean asMaster);

  /**
   * Stop the master. Here, anything created or started in {@link #start(boolean)} should be cleaned
   * up and shutdown.
   */
  void stop();
}
