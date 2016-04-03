/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The file block descriptor.
 */
@NotThreadSafe
public final class FileBlockInfo {
  private BlockInfo mBlockInfo = new BlockInfo();
  private long mOffset;
  private List<WorkerNetAddress> mUfsLocations = Lists.newArrayList();

  /**
   * Creates a new instance of {@link FileBlockInfo}.
   */
  public FileBlockInfo() {}

  /**
   * Creates a new instance of {@link FileBlockInfo} from a thrift representation.
   *
   * @param fileBlockInfo the thrift representation of a file block descriptor
   */
  protected FileBlockInfo(alluxio.thrift.FileBlockInfo fileBlockInfo) {
    mBlockInfo = new BlockInfo(fileBlockInfo.getBlockInfo());
    mOffset = fileBlockInfo.getOffset();
    mUfsLocations = new ArrayList<WorkerNetAddress>();
    for (alluxio.thrift.WorkerNetAddress ufsLocation : fileBlockInfo.getUfsLocations()) {
      mUfsLocations.add(new WorkerNetAddress(ufsLocation));
    }
  }

  /**
   * @return the block info
   */
  public BlockInfo getBlockInfo() {
    return mBlockInfo;
  }

  /**
   * @return the offset
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the UFS locations
   */
  public List<WorkerNetAddress> getUfsLocations() {
    return mUfsLocations;
  }

  /**
   * @param blockInfo the block info to use
   * @return the file block descriptor
   */
  public FileBlockInfo setBlockInfo(BlockInfo blockInfo) {
    Preconditions.checkNotNull(blockInfo);
    mBlockInfo = blockInfo;
    return this;
  }

  /**
   * @param offset the offset to use
   * @return the file block descriptor
   */
  public FileBlockInfo setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  /**
   * @param ufsLocations the UFS locations to use
   * @return the file block descriptor
   */
  public FileBlockInfo setUfsLocations(List<WorkerNetAddress> ufsLocations) {
    Preconditions.checkNotNull(ufsLocations);
    mUfsLocations = ufsLocations;
    return this;
  }

  /**
   * @return thrift representation of the file block descriptor
   */
  protected alluxio.thrift.FileBlockInfo toThrift() {
    List<alluxio.thrift.WorkerNetAddress> ufsLocations =
        new ArrayList<alluxio.thrift.WorkerNetAddress>();
    for (WorkerNetAddress ufsLocation : mUfsLocations) {
      ufsLocations.add(ufsLocation.toThrift());
    }
    return new alluxio.thrift.FileBlockInfo(mBlockInfo.toThrift(), mOffset, ufsLocations);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileBlockInfo)) {
      return false;
    }
    FileBlockInfo that = (FileBlockInfo) o;
    return mBlockInfo.equals(that.mBlockInfo) && mOffset == that.mOffset
        && mUfsLocations.equals(that.mUfsLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockInfo, mOffset, mUfsLocations);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockInfo", mBlockInfo).add("offset", mOffset)
        .add("ufsLocations", mUfsLocations).toString();
  }
}
