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
import alluxio.thrift.ExportJournalTResponse;

import com.google.common.base.Preconditions;

/**
 * Response for the exportJournal RPC.
 */
public class ExportJournalResponse {
  // The URI of the backed up file.
  private AlluxioURI mBackupUri;
  // The hostname of the master that did the export.
  private String mHostname;

  /**
   * @param backupUri the URI of the backed up file
   * @param hostname the hostname of the master that did the export
   */
  public ExportJournalResponse(AlluxioURI backupUri, String hostname) {
    mBackupUri = Preconditions.checkNotNull(backupUri, "backupUri");
    mHostname = Preconditions.checkNotNull(hostname, "hostname");
  }

  /**
   * @param tResp thrift options
   * @return wire type options corresponding to the thrift options
   */
  public static ExportJournalResponse fromThrift(ExportJournalTResponse tResp) {
    return new ExportJournalResponse(new AlluxioURI(tResp.getBackupUri()), tResp.getHostname());
  }

  /**
   * @return the thrift options corresponding to these options
   */
  public ExportJournalTResponse toThrift() {
    return new ExportJournalTResponse().setBackupUri(mBackupUri.toString()).setHostname(mHostname);
  }

  /**
   * @return the URI of the backed up file
   */
  public AlluxioURI getBackupUri() {
    return mBackupUri;
  }

  /**
   * @return the hostname of the master that did the export
   */
  public String getHostname() {
    return mHostname;
  }
}
