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

package alluxio.master.backup;

import alluxio.exception.AlluxioException;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.master.StateLockOptions;
import alluxio.wire.BackupStatus;

import java.io.IOException;

/**
 * Interface for backup operations.
 */
public interface BackupOps {
  /**
   * Takes a backup.
   *
   * Note: If backup request ask for async execution, it will return after initiating the backup.
   * Status for the in-progress backup will be returned. {@link #getBackupStatus} should be called
   * for querying the status of the on-going backup.
   *
   * Note: When leader has no secondary in an HA cluster, it will reject backup. This could be
   * allowed by passing "AllowLeader" option in the request.
   *
   * @param request the backup request
   * @param stateLockOptions the state lock options during the backup
   * @return the backup status response
   * @throws IOException if backup fails
   */
  BackupStatus backup(BackupPRequest request, StateLockOptions stateLockOptions)
      throws AlluxioException;

  /**
   * Used to query the status of a backup.
   *
   * @param statusPRequest status request
   * @return the status of the latest backup
   * @throws IOException
   */
  BackupStatus getBackupStatus(BackupStatusPRequest statusPRequest) throws AlluxioException;
}
