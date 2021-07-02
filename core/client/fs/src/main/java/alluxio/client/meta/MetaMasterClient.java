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

package alluxio.client.meta;

import alluxio.Client;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.wire.BackupStatus;
import alluxio.wire.ConfigCheckReport;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

/**
 * Interface for a meta master client.
 */
public interface MetaMasterClient extends Client {

  /**
   * Takes a backup.
   *
   * Note: If backup request ask for async execution, it will return after initiating the backup.
   *       Status for the in-progress backup will be returned. {@link #getBackupStatus} should
   *       be called for querying the status of the on-going backup.
   *
   * Note: When leader has no secondary in an HA cluster, it will reject backup. This could
   *       be allowed by passing "AllowLeader" option in the request.
   *
   * @param backupRequest the backup request
   * @return status of backup
   * @throws IOException
   */
  BackupStatus backup(BackupPRequest backupRequest) throws IOException;

  /**
   * Queries the status of a backup.
   *
   * @param backupId backup id
   * @return the status of the latest backup
   * @throws IOException
   */
  BackupStatus getBackupStatus(UUID backupId) throws IOException;

  /**
   * Gets the server-side configuration check report.
   *
   * @return configuration check report
   */
  ConfigCheckReport getConfigReport() throws IOException;

  /**
   * @param masterInfoFields optional list of fields to query; if null all fields will be queried
   * @return the requested master info
   */
  MasterInfo getMasterInfo(Set<MasterInfoField> masterInfoFields) throws IOException;

  /**
   * Creates a checkpoint in the primary master journal system.
   *
   * @return the hostname of the master that did the checkpoint
   */
  String checkpoint() throws IOException;
}
