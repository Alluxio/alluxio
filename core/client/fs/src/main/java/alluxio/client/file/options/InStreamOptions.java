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

package alluxio.client.file.options;

import alluxio.client.file.URIStatus;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information for reading a file. This is an internal options class which contains information
 * from {@link OpenFileOptions} as well as the {@link URIStatus} being read. In addition to
 * providing access to the fields, it provides convenience functions for various nested
 * fields and creating {@link alluxio.proto.dataserver.Protocol.ReadRequest}s.
 */
@NotThreadSafe
// TODO(calvin): Rename this class
public final class InStreamOptions {
  private final URIStatus mStatus;
  private final OpenFileOptions mOptions;

  /**
   * Creates with the default {@link OpenFileOptions}.
   * @param status the file to create the options for
   */
  public InStreamOptions(URIStatus status) {
    this(status, OpenFileOptions.defaults());
  }

  /**
   * Creates based on the arguments provided.
   * @param status the file to create the options for
   * @param options the {@link OpenFileOptions} to use
   */
  public InStreamOptions(URIStatus status, OpenFileOptions options) {
    mStatus = status;
    mOptions = options;
  }

  /**
   * @return the {@link OpenFileOptions} associated with the instream
   */
  public OpenFileOptions getOptions() {
    return mOptions;
  }

  /**
   * @return the {@link URIStatus} associated with the instream
   */
  public URIStatus getStatus() {
    return mStatus;
  }

  /**
   * @param blockId id of the block
   * @return the block info associated with the block id, or an exception if the block id is not
   * associated with the file.
   */
  public BlockInfo getBlockInfo(long blockId) {
    return mStatus.getFileBlockInfos().stream().map(FileBlockInfo::getBlockInfo)
        .filter(blockInfo -> blockInfo.getBlockId() == blockId).findFirst().get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InStreamOptions)) {
      return false;
    }
    InStreamOptions that = (InStreamOptions) o;
    return Objects.equal(mStatus, that.mStatus) && Objects.equal(mOptions, that.mOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mStatus, mOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("URIStatus", mStatus).add("OpenFileOptions", mOptions)
        .toString();
  }
}
