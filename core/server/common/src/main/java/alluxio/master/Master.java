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

package alluxio.master;

import alluxio.master.journal.JournalCheckpointStreamable;
import alluxio.master.journal.JournalInputStream;
import alluxio.proto.journal.Journal.JournalEntry;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
   * @return the master dependencies
   */
  Set<Class<?>> getDependencies();

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
   * Updates the master state for being a leader master.
   *
   * This hook is called when the master becomes a leader. The master should not be running when
   * this method is called.
   */
  void transitionToLeader();
}
