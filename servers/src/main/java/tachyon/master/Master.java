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

package tachyon.master;

import java.io.IOException;

import org.apache.thrift.TProcessor;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalInputStream;
import tachyon.master.journal.JournalSerializable;

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
   * @throws IOException
   */
  void processJournalCheckpoint(JournalInputStream inputStream) throws IOException;

  /**
   * Processes a journal entry. These entries follow the checkpoint entries.
   *
   * @param entry the entry to process to update the state of the master.
   * @throws IOException
   */
  void processJournalEntry(JournalEntry entry) throws IOException;

  /**
   * Start the master, as the leader master or standby. Here, the master should initialize state and
   * possibly start threads required for operation.
   *
   * If the parameter asMaster is true, the master should also initialize the journal and write the
   * checkpoint file.
   *
   * @param asMaster if true, the master should behave as the leader master in the system. If false,
   *        the master should act as a standby master.
   * @throws IOException
   */
  void start(boolean asMaster) throws IOException;

  /**
   * Stop the master. Here, anything created or started in {@link #start(boolean)} should be cleaned
   * up and shutdown.
   *
   * @throws IOException
   */
  void stop() throws IOException;
}
