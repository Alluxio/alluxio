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

import alluxio.master.job.JobMaster;
import alluxio.master.journal.JournalSystem;

import java.net.ServerSocket;

/**
 * A job master in the Alluxio system.
 */
public abstract class JobMasterProcess extends MasterProcess {

  /**
   * Creates a new {@link JobMasterProcess} to complement an AlluxioMasterProcess.
   *
   * @param journalSystem the journaling system
   * @param rpcBindSocket the socket whose address the rpc server will eventually bind to
   * @param webBindSocket the socket whose address the web server will eventually bind to
   */
  public JobMasterProcess(JournalSystem journalSystem, ServerSocket rpcBindSocket,
      ServerSocket webBindSocket) {
    super(journalSystem, rpcBindSocket, webBindSocket);
  }

  /**
   * @return the {@link JobMaster} object from the process
   */
  public abstract JobMaster getJobMaster();
}
