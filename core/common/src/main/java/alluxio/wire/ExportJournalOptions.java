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

import alluxio.thrift.ExportJournalTOptions;

import com.google.common.base.Preconditions;

/**
 * Options for exporting the Alluxio master journal.
 */
public class ExportJournalOptions {
  // URI to export the journal to.
  private String mTargetDirectoryUri;

  /**
   * @param targetDirectoryUri uri of the directory to export the journal to
   */
  public ExportJournalOptions(String targetDirectoryUri) {
    mTargetDirectoryUri = Preconditions.checkNotNull(targetDirectoryUri, "targetDirectoryUri");
  }

  /**
   * @param tOpts thrift options
   * @return wire type options corresponding to the thrift options
   */
  public static ExportJournalOptions fromThrift(ExportJournalTOptions tOpts) {
    return new ExportJournalOptions(tOpts.getTargetDirectoryUri());
  }

  /**
   * @return the thrift options corresponding to these options
   */
  public ExportJournalTOptions toThrift() {
    return new ExportJournalTOptions().setTargetDirectoryUri(mTargetDirectoryUri);
  }

  /**
   * @return the uri to export the journal to
   */
  public String getTargetDirectoryUri() {
    return mTargetDirectoryUri;
  }
}
