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

package alluxio.wire;

import alluxio.AlluxioURI;
import alluxio.grpc.BackupPResponse;

import com.google.common.base.Preconditions;

/**
 * Response for the backup RPC.
 */
public class BackupResponse {
  // The URI of the backed up file.
  private AlluxioURI mBackupUri;
  // The hostname of the master that did the backup.
  private String mHostname;
  // The entry count in backup.
  private long mEntryCount;

  /**
   * @param backupUri the URI of the backed up file
   * @param hostname the hostname of the master that did the backup
   * @param entryCount the entry count in backup
   */
  public BackupResponse(AlluxioURI backupUri, String hostname, long entryCount) {
    mBackupUri = Preconditions.checkNotNull(backupUri, "backupUri");
    mHostname = Preconditions.checkNotNull(hostname, "hostname");
    mEntryCount = entryCount;
  }

  /**
   * @param pResp proto options
   * @return wire type options corresponding to the proto options
   */
  public static BackupResponse fromProto(BackupPResponse pResp) {
    return new BackupResponse(new AlluxioURI(
        pResp.getBackupUri()),
        pResp.getHostname(),
        pResp.getEntryCount());
  }

  /**
   * @return the proto options corresponding to these options
   */
  public BackupPResponse toProto() {
    return BackupPResponse.newBuilder()
        .setBackupUri(mBackupUri.toString())
        .setHostname(mHostname)
        .setEntryCount(mEntryCount)
        .build();
  }

  /**
   * @return the URI of the backed up file
   */
  public AlluxioURI getBackupUri() {
    return mBackupUri;
  }

  /**
   * @return the hostname of the master that did the backup
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * @return the entry count in backup
   */
  public long getEntryCount() {
    return mEntryCount;
  }
}
