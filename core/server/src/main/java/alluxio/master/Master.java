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

package alluxio.master;

import java.io.IOException;
import java.util.Map;

import org.apache.thrift.TProcessor;

import alluxio.master.journal.JournalCheckpointStreamable;
import alluxio.master.journal.JournalInputStream;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.proto.journal.Journal.JournalEntry;

/**
 * This interface contains common operations for all masters.
 */
public interface Master extends JournalCheckpointStreamable {
  /**
   * @return a map from service names to {@link TProcessor}s that serve RPCs for this master
   */
  Map<String, TProcessor> getServices();

  /**
   * @return the master's name
   */
  String getName();

  /**
   * Processes the journal checkpoint file and applies the entries to the master.
   *
   * @param inputStream the input stream for the journal checkpoint file
   * @throws IOException if I/O error occurs
   */
  void processJournalCheckpoint(JournalInputStream inputStream) throws IOException;

  /**
   * Processes a journal entry and applies it to the master. These entries follow the checkpoint
   * entries.
   *
   * @param entry the entry to process to update the state of the master
   * @throws IOException if I/O error occurs
   */
  void processJournalEntry(JournalEntry entry) throws IOException;

  /**
   * Starts the master, as the leader master or the standby master. Here, the master should
   * initialize state and possibly start threads required for operation.
   *
   * If isLeader is true, the master should also initialize the journal and write the checkpoint
   * file.
   *
   * @param isLeader if true, the master should behave as the leader master in the system. If false,
   *        the master should act as a standby master.
   * @throws IOException if I/O error occurs
   */
  void start(boolean isLeader) throws IOException;

  /**
   * Stops the master. Here, anything created or started in {@link #start(boolean)} should be
   * cleaned up and shutdown.
   *
   * @throws IOException if I/O error occurs
   */
  void stop() throws IOException;

  /**
   * Provides the master with a {@link ReadWriteJournal} capable of writing, in preparation to
   * starting as the leader. This enables transitioning from standby master to leader master without
   * having to construct a new master object.
   *
   * @param journal the {@link ReadWriteJournal} capable of writing
   */
  void upgradeToReadWriteJournal(ReadWriteJournal journal);
}
