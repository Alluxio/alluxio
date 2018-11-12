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

package alluxio.master.file.options;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions
    extends alluxio.file.options.CreateFileOptions<CreateFileOptions> {

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  private CreateFileOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mMountPoint = false;
    mOwner = "";
    mGroup = "";
    mAcl = Collections.emptyList();
    mOperationTimeMs = System.currentTimeMillis();
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mReplicationDurable = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE);
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    mMode = ModeUtils.applyFileUMask(Mode.defaults());
    mCacheable = false;
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;
  }

  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public CreateFileOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated options object
   */
  public CreateFileOptions setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  @Override
  protected CreateFileOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mReplicationDurable, that.mReplicationDurable)
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        && Objects.equal(mCacheable, that.mCacheable);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mReplicationDurable,
        mReplicationMax, mReplicationMin, mCacheable);
  }

  @Override
  public String toString() {
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes)
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        .add("cacheable", mCacheable).toString();
  }
}
